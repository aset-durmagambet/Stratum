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
from collections import deque

# Настройки логирования
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Настройки пула и ASIC
POOL_HOST = "stratum.braiins.com"
POOL_PORT = 3333
POOL_WORKER = "AssetDurmagambet.worker1"
POOL_PASSWORD = "x"
ASIC_PROXY_HOST = "0.0.0.0"
ASIC_PROXY_PORT = 3333

class StratumHandler(socketserver.BaseRequestHandler):
    def handle(self):
        ip, port = self.client_address
        logger.info(f"Antminer S19 подключен с IP {ip}:{port}")
        self.server.proxy.asic_client = self
        last_seen = time.time()

        def monitor_asic():
            while True:
                time.sleep(10)
                if time.time() - last_seen > 60:
                    logger.warning("Antminer неактивен более 60 секунд")
                    break
        threading.Thread(target=monitor_asic, daemon=True).start()

        while self.server.proxy.delayed_jobs:
            job = self.server.proxy.delayed_jobs.popleft()
            logger.info("Отправка отложенного задания в Antminer")
            self.send_job(job)

        if self.server.proxy.job_params:
            try:
                diff_msg = {
                    "id": None,
                    "method": "mining.set_difficulty",
                    "params": [8192]
                }
                self.send_job(diff_msg)

                set_extranonce_msg = {
                    "id": None,
                    "method": "mining.set_extranonce",
                    "params": [self.server.proxy.extranonce1, 4]
                }
                self.send_job(set_extranonce_msg)
            except Exception as e:
                logger.error(f"Ошибка при первичной инициализации ASIC: {e}")

        buffer = ""
        while True:
            try:
                data = self.request.recv(8192).decode("utf-8")
                if not data:
                    logger.debug("Antminer S19 отключен")
                    break
                last_seen = time.time()
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
                                logger.info(f"[SHARE] От ASIC: {line}")
                                self.server.proxy.total_jobs_completed += 1
                                # Отправка решения в пул
                                self.server.proxy.pool_socket.sendall((line + "\n").encode("utf-8"))
                                # Увеличиваем счетчик отправленных шаров
                                self.server.proxy.total_shares_sent_to_pool += 1  
                                logger.info("[ACCEPTED] ASIC отправил решение по заданию")
                        except json.JSONDecodeError:
                            logger.debug(f"Неверный JSON от Antminer S19: {line}")
            except Exception as e:
                logger.error(f"Ошибка связи с Antminer: {e}")
                break
        self.server.proxy.asic_client = None

    def send_job(self, job):
        try:
            if not self.request:
                logger.warning("[SEND JOB] Подключение к ASIC отсутствует!")
                return

            if job["method"] != "mining.notify":
                logger.warning("[SEND JOB] Неподдерживаемый тип задания")
                return

            params = job["params"]
            job_id = params[0]
            prevhash = params[1]
            coinb1 = params[2]
            coinb2 = params[3]
            merkle_branch = params[4]
            version = params[5]
            nbits = params[6]
            ntime = params[7]
            clean_jobs = params[8]

            # Получаем лучший extranonce2 для текущего job_id
            extranonce2 = self.server.proxy.extranonce2_map.get(job_id, "00000000")
            extranonce1 = self.server.proxy.extranonce1

            # Сборка coinbase
            coinbase_hex = coinb1 + extranonce1 + extranonce2 + coinb2
            coinbase_bin = binascii.unhexlify(coinbase_hex)
            coinbase_hash = hashlib.sha256(hashlib.sha256(coinbase_bin).digest()).digest()

            # Логируем состав coinbase
            logger.debug(f"[COINBASE CONSTRUCTED]\n  coinb1       = {coinb1}\n  extranonce1  = {extranonce1}\n  extranonce2  = {extranonce2}\n  coinb2       = {coinb2}")
            logger.debug(f"  coinbase     = {coinbase_hex}")
            logger.debug(f"  coinbase_hash= {binascii.hexlify(coinbase_hash).decode()}")

            # Сборка нового merkle root
            merkle_root = coinbase_hash
            for branch in merkle_branch:
                merkle_root = hashlib.sha256(hashlib.sha256(merkle_root + binascii.unhexlify(branch)).digest()).digest()
            merkle_root_hex = merkle_root.hex()
            logger.info(f"[MERKLE ROOT] job_id={job_id} → {merkle_root_hex}")

            # Создаём новое задание для ASIC с теми же полями
            notify_for_asic = {
                "method": "mining.notify",
                "params": [
                    job_id,
                    prevhash,
                    coinb1,
                    coinb2,
                    merkle_branch,
                    version,
                    nbits,
                    ntime,
                    clean_jobs
                ]
            }

            # Проверка на дублирование
            if job_id == self.server.proxy.last_job_id and extranonce2 == self.server.proxy.last_extranonce2:
                logger.info("[NO CHANGE] Задание не изменилось, не отправляем повторно")
                return

            # Обновляем статус
            self.server.proxy.last_job_id = job_id
            self.server.proxy.last_extranonce2 = extranonce2
            self.server.proxy.total_jobs_sent += 1

            logger.info(f"[SEND TO ASIC]\n  job_id      = {job_id}\n  extranonce2 = {extranonce2}")
            self.request.sendall((json.dumps(notify_for_asic) + "\n").encode("utf-8"))
            time.sleep(0.1)

        except Exception as e:
            logger.error(f"Ошибка отправки задания на Antminer: {e}")


class StratumProxy:
    def __init__(self):
        self.pool_socket = None
        self.extranonce1 = "00000000"
        self.extranonce2 = "00000000"
        self.asic_client = None
        self.running = True
        self.job_params = {}
        self.last_prevhash = None
        self.delayed_jobs = deque(maxlen=100)

        # Новые счетчики
        self.total_shares_accepted = 0  # Количество принятых шаров пулом
        self.total_shares_rejected = 0  # Количество отклоненных шаров пулом
        self.total_jobs_sent = 0        # Количество отправленных заданий
        self.total_jobs_completed = 0   # Количество завершенных заданий
        self.total_shares_sent_to_pool = 0  # Количество отправленных шаров в пул

        # Новые переменные для отслеживания последнего задания
        self.last_job_id = None
        self.last_extranonce2 = None

        # Добавлено для логики фильтрации и защиты от повторов
        self.current_job_id = None
        self.extranonce2_map = {}
        self.last_job_id_sent_to_asic = None

        threading.Thread(target=self.print_stats, daemon=True).start()

    def print_stats(self):
        while True:
            logger.info("\n======= СТАТИСТИКА =======\n"
                        f"  Отправлено заданий  : {self.total_jobs_sent}\n"
                        f"  Принято шар от ASIC : {self.total_shares_accepted}\n"
                        f"  Отправлено шаров на пул : {self.total_shares_sent_to_pool}\n"
                        f"  Принято шар пулом    : {self.total_shares_accepted}\n"
                        f"  Отклонено шар пулом  : {self.total_shares_rejected}\n"
                        f"  Принято заданий      : {self.total_jobs_completed}\n"
                        f"  Отложено заданий     : {len(self.delayed_jobs)}\n"
                        "==========================")
            time.sleep(60)

    def calculate_merkle_root(self, coinb1, coinb2, extranonce1, extranonce2, merkle_branch):
        coinbase = binascii.unhexlify(coinb1) + binascii.unhexlify(extranonce1) + binascii.unhexlify(extranonce2) + binascii.unhexlify(coinb2)
        coinbase_hash = hashlib.sha256(hashlib.sha256(coinbase).digest()).digest()
        current_hash = coinbase_hash
        for branch in merkle_branch:
            current_hash = hashlib.sha256(hashlib.sha256(current_hash + binascii.unhexlify(branch)).digest()).digest()
        return current_hash.hex()

    def handle_pool_message(self, message):
        try:
            data = json.loads(message)
            logger.debug(f"Обработка сообщения от пула: {data}")

            if data.get("id") == 1 and "result" in data:
                self.extranonce1 = data["result"][1]
                logger.info(f"Получен extranonce1 от пула: {self.extranonce1}")

            elif data.get("id") == 2 and "result" in data:
                logger.info("Авторизация на пуле прошла успешно." if data["result"] else "Авторизация не удалась")

            elif data.get("method") == "mining.notify":
                params = data.get("params", [])
                if len(params) >= 9:
                    job_id, prevhash, coinb1, coinb2, merkle_branch, version, nbits, ntime, clean_jobs = params
                    self.last_prevhash = prevhash
                    self.job_params = {"version": version, "nbits": nbits, "ntime": ntime}

                    # === Фильтрация extranonce2 по job_id ===
                    valid = []
                    for i in range(1000):
                        en2 = f"{i:08x}"
                        root = self.calculate_merkle_root(coinb1, coinb2, self.extranonce1, en2, merkle_branch)
                        total = sum(int.from_bytes(binascii.unhexlify(root)[i:i+4], 'big') for i in range(0, 16, 4))
                        if total < 2**31.95:
                            valid.append((en2, total))
                    valid.sort(key=lambda x: x[1])

                    self.current_job_id = job_id
                    self.extranonce2 = valid[0][0] if valid else "00000000"
                    self.extranonce2_map[job_id] = self.extranonce2

                    logger.info(f"[FILTERED] job_id={job_id} → best extranonce2: {self.extranonce2}")
                    logger.info("Топ значений:\n" + "\n".join([f"  - {v[0]} -> вес: {v[1]}" for v in valid[:5]]))

                    notify = {
                        "method": "mining.notify",
                        "params": [
                            job_id,
                            prevhash,
                            coinb1,
                            coinb2,
                            merkle_branch,
                            version,
                            nbits,
                            ntime,
                            clean_jobs
                        ]
                    }

                    # === Умный сброс и отправка ===
                    send_required = False
                    if clean_jobs or job_id != self.last_job_id_sent_to_asic:
                        send_required = True
                        self.last_job_id_sent_to_asic = job_id

                    if send_required:
                        if self.asic_client:
                            logger.info(f"[DISPATCH] Отправка нового задания в ASIC: job_id={job_id}, clean_jobs={clean_jobs}")
                            self.asic_client.send_job(notify)
                        else:
                            logger.info(f"[QUEUE] ASIC не подключён — задание в очередь: job_id={job_id}")
                            self.delayed_jobs.append(notify)
                    else:
                        logger.info(f"[SKIP] Задание уже отправлялось (job_id={job_id}), clean_jobs={clean_jobs} — пропущено.")

            elif data.get("id") and "result" in data:
                share_result = data["result"]
                if share_result is True:
                    self.total_shares_accepted += 1
                    logger.info(f"[SHARE ACCEPTED] id={data['id']}, total={self.total_shares_accepted}")
                else:
                    self.total_shares_rejected += 1
                    logger.warning(f"[SHARE REJECTED] id={data['id']}, rejected={self.total_shares_rejected}")

        except Exception as e:
            logger.error(f"Ошибка при обработке сообщения от пула: {e}")

    def listen_to_pool(self):
        buffer = b""
        while self.running:
            try:
                chunk = self.pool_socket.recv(8192)
                if not chunk:
                    break
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    self.handle_pool_message(line.decode("utf-8").strip())
            except Exception as e:
                logger.error(f"Ошибка при получении данных от пула: {e}")
                break

    def connect_to_pool(self):
        while self.running:
            try:
                logger.info("Подключение к пулу...")
                self.pool_socket = socket.create_connection((POOL_HOST, POOL_PORT))
                logger.info("Подключено к пулу")

                subscribe_request = json.dumps({"id": 1, "method": "mining.subscribe", "params": ["bmminer/1.0.0"]}) + "\n"
                self.pool_socket.sendall(subscribe_request.encode("utf-8"))

                auth_request = json.dumps({"id": 2, "method": "mining.authorize", "params": [POOL_WORKER, POOL_PASSWORD]}) + "\n"
                self.pool_socket.sendall(auth_request.encode("utf-8"))

                self.listen_to_pool()
            except Exception as e:
                logger.error(f"Ошибка подключения к пулу: {e}")
                time.sleep(5)

    def start(self):
        threading.Thread(target=self.connect_to_pool, daemon=True).start()

def start_asic_server(proxy):
    try:
        server = socketserver.ThreadingTCPServer((ASIC_PROXY_HOST, ASIC_PROXY_PORT), StratumHandler)
        server.proxy = proxy
        ip, port = server.server_address
        logger.info(f"Сервер для Antminer S19 запущен на {ip}:{port}")
        server.serve_forever()
    except Exception as e:
        logger.critical(f"Ошибка запуска сервера для ASIC: {e}")

if __name__ == "__main__":
    proxy = StratumProxy()
    threading.Thread(target=start_asic_server, args=(proxy,), daemon=True).start()
    proxy.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Завершение по Ctrl+C")
