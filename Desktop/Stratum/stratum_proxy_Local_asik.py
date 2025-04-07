import socket
import json
import threading
import time
import hashlib
import binascii
import numpy as np
from queue import Queue
import logging
import socketserver
import requests

# Настройки логирования
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

l

# Настройки пула и ASIC
POOL_HOST = "stratum.braiins.com"
POOL_PORT = 3333
POOL_WORKER = "AssetDurmagambet.worker1"
POOL_PASSWORD = "77Aset88!"
ASIC_HOST = "37.208.47.150"
ASIC_API_PORT = 80
ASIC_PROXY_PORT = 3333
API_TOKEN = "ryXhYEjwc1I32kOx"  # Замените на действительный токен

class StratumHandler(socketserver.BaseRequestHandler):
    def handle(self):
        logger.debug(f"Antminer S19 подключен: {self.client_address}")
        self.server.proxy.asic_client = self
        buffer = ""
        while True:
            try:
                data = self.request.recv(8192).decode("utf-8")
                if not data:
                    logger.debug("Antminer S19 отключен")
                    break
                buffer += data
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if line:
                        logger.debug(f"Получено от Antminer S19: {line}")
                        try:
                            msg = json.loads(line)
                            if "method" in msg and msg["method"] == "mining.subscribe":
                                response = {
                                    "id": msg["id"],
                                    "result": [["mining.notify", "ae6812eb4cd7735a"], self.server.proxy.extranonce1, 4],
                                    "error": None
                                }
                                self.request.sendall((json.dumps(response) + "\n").encode("utf-8"))
                            elif "method" in msg and msg["method"] == "mining.authorize":
                                response = {"id": msg["id"], "result": True, "error": None}
                                self.request.sendall((json.dumps(response) + "\n").encode("utf-8"))
                            elif "method" in msg and msg["method"] == "mining.submit":
                                logger.info(f"Доля отправлена от Antminer: {line}")
                                self.server.proxy.pool_socket.sendall((line + "\n").encode("utf-8"))
                                logger.debug(f"Переслано пулу: {line}")
                        except json.JSONDecodeError:
                            logger.debug(f"Неверный JSON от Antminer S19: {line}")
            except Exception as e:
                logger.error(f"Ошибка связи с Antminer: {e}")
                break
        self.server.proxy.asic_client = None

    def send_job(self, job):
        try:
            self.request.sendall((json.dumps(job) + "\n").encode("utf-8"))
           # logger.debug(f"Отправлено задание на Antminer S19: {job}")
        except Exception as e:
            logger.error(f"Ошибка отправки задания на Antminer: {e}")

def start_asic_server(proxy):
    server = socketserver.ThreadingTCPServer(("0.0.0.0", ASIC_PROXY_PORT), StratumHandler)
    server.proxy = proxy
    logger.info(f"Сервер для Antminer S19 запущен на порту {ASIC_PROXY_PORT}")
    server.serve_forever()

class StratumProxy:
    def __init__(self, pool_host, pool_port, asic_host, asic_port):
        self.pool_host = pool_host
        self.pool_port = pool_port
        self.asic_host = asic_host
        self.asic_port = asic_port
        self.pool_socket = None
        self.running = True
        self.extranonce1 = None
        self.extranonce2 = "00000000"
        self.task_queue = Queue()
        self.program_start_time = time.time()
        self.last_log_time = self.program_start_time
        self.current_job = None
        self.filtering_active = False
        self.last_accepted = 0
        self.last_check_time = self.program_start_time
        self.first_run = True
        self.asic_client = None
        self.last_pool_check_time = self.program_start_time
        self.job_params = {}

    def double_sha256(self, data):
        return hashlib.sha256(hashlib.sha256(data).digest()).digest()

    def merkle_root(self, coinb1, coinb2, extranonce1, extranonce2, merkle_branch):
        coinbase = binascii.unhexlify(coinb1) + binascii.unhexlify(extranonce1) + binascii.unhexlify(extranonce2) + binascii.unhexlify(coinb2)
        coinbase_hash = self.double_sha256(coinbase)
        current_hash = coinbase_hash
        for branch in merkle_branch:
            current_hash = self.double_sha256(current_hash + binascii.unhexlify(branch))
        return current_hash

    def extend_words(self, words):
        W = np.zeros(64, dtype=np.uint32)
        for i in range(4):
            W[i] = np.uint32(int.from_bytes(words[i], byteorder='big'))
        return np.sum(W, dtype=np.uint32)

    def is_valid_merkle(self, words):
        total_w = self.extend_words(words)
        return total_w < 2**31.95

    def filter_extranonce1(self, job_id, prevhash, coinb1, coinb2, merkle_branch, extranonce1_start):
        self.filtering_active = True
        extranonce1_int = int(extranonce1_start, 16)
        max_extranonce1 = extranonce1_int + 2**32

        logger.debug(f"Начало фильтрации: job_id={job_id}, extranonce1_start={extranonce1_start}")
        while self.running and self.current_job == (job_id, prevhash, coinb1, coinb2, tuple(merkle_branch)) and extranonce1_int < max_extranonce1:
            extranonce1 = f"{extranonce1_int:08x}"
            merkle_root = self.merkle_root(coinb1, coinb2, extranonce1, self.extranonce2, merkle_branch)
            last_16_bytes = merkle_root[-16:]
            words = [last_16_bytes[i:i+4] for i in range(0, 16, 4)]
            
            if self.is_valid_merkle(words):
                task = {
                    "id": None,
                    "method": "mining.notify",
                    "params": [
                        job_id, prevhash, coinb1, coinb2, merkle_branch,
                        self.job_params.get("version", "20000000"),
                        self.job_params.get("nbits", "170362d3"),
                        self.job_params.get("ntime", f"{int(time.time()):08x}"),
                        False
                    ]
                }
                self.task_queue.put((extranonce1, task))

            extranonce1_int += 1

        logger.debug(f"Конец фильтрации для job_id={job_id}")
        self.filtering_active = False

    def process_queue(self):
        while self.running:
            if not self.task_queue.empty() and self.asic_client:
                extranonce1, job = self.task_queue.get()
                self.extranonce1 = extranonce1
                self.asic_client.send_job(job)
            time.sleep(1)

    def connect_to_pool(self):
        while self.running:
            logger.debug("Подключение к пулу")
            try:
                self.pool_socket = socket.create_connection((self.pool_host, self.pool_port))
                subscribe_request = json.dumps({"id": 2, "method": "mining.subscribe", "params": ["bmminer/1.0.0"]}) + "\n"
                self.pool_socket.sendall(subscribe_request.encode("utf-8"))
                auth_request = json.dumps({
                    "id": 1,
                    "method": "mining.authorize",
                    "params": [POOL_WORKER, POOL_PASSWORD]
                }) + "\n"
                self.pool_socket.sendall(auth_request.encode("utf-8"))
                logger.debug("Успешное подключение к пулу")
                self.listen_to_pool()
            except Exception as e:
                logger.error(f"Ошибка подключения к пулу: {e}")
                if self.running:
                    logger.info("Попытка переподключения через 5 секунд...")
                    time.sleep(5)

    def listen_to_pool(self):
        buffer = b""
        while self.running:
            try:
                chunk = self.pool_socket.recv(8192)
                if not chunk:
                    logger.error("Соединение с пулом разорвано")
                    break
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    data = line.decode("utf-8").strip()
                    if data:
                        self.handle_pool_message(data)
            except Exception as e:
                logger.error(f"Ошибка при получении данных от пула: {e}")
                break

    def handle_pool_message(self, data):
        if not data or len(data) < 10:
            return
        try:
            json_data = json.loads(data)
            if "method" in json_data and json_data["method"] == "mining.notify":
                params = json_data["params"]
                job_id, prevhash, coinb1, coinb2, merkle_branch, version, nbits, ntime, clean_jobs = params
                self.current_job = (job_id, prevhash, coinb1, coinb2, tuple(merkle_branch))
                self.job_params = {"version": version, "nbits": nbits, "ntime": ntime}
                while not self.task_queue.empty():
                    self.task_queue.get()
                if not self.filtering_active:
                    threading.Thread(target=self.filter_extranonce1, args=(job_id, prevhash, coinb1, coinb2, merkle_branch, self.extranonce1 or "00000001")).start()
            elif "result" in json_data and json_data["id"] == 1:
                logger.info(f"Авторизация успешна: {json_data}")
            elif "result" in json_data and json_data["id"] == 2:
                result = json_data["result"]
                self.extranonce1 = result[1] if result[1] != "00000000" else "00000001"
                logger.info(f"Подписка успешна: extranonce1={self.extranonce1}, extranonce2=00000000")
        except json.JSONDecodeError:
            logger.debug(f"Неверный JSON: {data}")

    def get_shares_info(self):
        """Disabled until correct API endpoint is found."""
        logger.info("Статистика пула временно отключена из-за неверного API endpoint.")
        return None

    def start(self):
        thread = threading.Thread(target=self.connect_to_pool)
        thread.start()
        threading.Thread(target=self.process_queue).start()

if __name__ == "__main__":
    proxy = StratumProxy(POOL_HOST, POOL_PORT, ASIC_HOST, ASIC_API_PORT)
    threading.Thread(target=start_asic_server, args=(proxy,)).start()
    proxy.start()
    time.sleep(5)
    while True:
        # proxy.get_shares_info()  # Disabled until API is fixed
        time.sleep(60)