import socket
import threading
import json
import hashlib
import binascii
import logging
import time
import sqlite3
import os
import signal
import psutil

from socketserver import ThreadingMixIn, TCPServer, StreamRequestHandler

ASIC_PROXY_HOST = '0.0.0.0'
ASIC_PROXY_PORT = 3333
POOL_HOST = 'stratum.braiins.com'
POOL_PORT = 3333
VERSION = "1.0.0"

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("stratum_proxy")

DB_PATH = "/root/Stratum/submits.db"

# Глобальные переменные для контроля предупреждений по HEX
last_hex_warning_time = 0
hex_warning_shown = False

def ensure_port_available(port):
    for conn in psutil.net_connections():
        if conn.laddr.port == port:
            try:
                proc = psutil.Process(conn.pid)
                logger.warning(f"[PORT IN USE] Убиваем процесс PID={conn.pid}, использующий порт :{port}")
                proc.terminate()
                proc.wait(timeout=3)
                time.sleep(2)
            except Exception as e:
                logger.error(f"Ошибка при завершении процесса: {e}")
            break

    while True:
        port_in_use = any(p.laddr.port == port for p in psutil.net_connections())
        if not port_in_use:
            logger.info(f"[PORT] Порт {port} освободился.")
            break
        time.sleep(1)

class ThreadedTCPServer(ThreadingMixIn, TCPServer):
    allow_reuse_address = True

class ASICHandler(StreamRequestHandler):
    def handle(self):
        logger.info(f"[ASIC] Подключение от {self.client_address[0]}")
        self.server.asic_socket = self.request
        while True:
            try:
                data = self.request.recv(4096).decode('utf-8')
                if data:
                    logger.info(f"[ASIC DATA] Получено от ASIC: {data}")
                    try:
                        msg = json.loads(data)
                        if msg.get("method") == "mining.configure":
                            response = {
                                "id": msg["id"],
                                "result": {"version-rolling": True},
                                "error": None
                            }
                            self.request.sendall((json.dumps(response) + '\n').encode('utf-8'))
                            logger.info(f"[ASIC] Отправлен ответ на mining.configure: {response}")
                        elif msg.get("method") == "mining.subscribe":
                            # Ожидание получения extranonce1 и extranonce2_size от proxy
                            while not (hasattr(self.server, 'proxy') and 
                                       self.server.proxy.extranonce1 is not None and 
                                       self.server.proxy.extranonce2_size):
                                time.sleep(1)
                            response = {
                                "id": msg["id"],
                                "result": [["mining.notify", "ae6812eb4cd7735a"]],
                                "extranonce1": self.server.proxy.extranonce1,
                                "extranonce2_size": self.server.proxy.extranonce2_size
                            }
                            self.request.sendall((json.dumps(response) + '\n').encode('utf-8'))
                            logger.info(f"[ASIC] Отправлен ответ на mining.subscribe: {response}")
                            # После подписки отправляем авторизацию
                            auth_msg = {"id": msg["id"] + 1, "method": "mining.authorize", "params": ["worker", "x"]}
                            self.server.proxy._send_to_pool(auth_msg)
                            self.request.sendall((json.dumps(auth_msg) + '\n').encode('utf-8'))
                            logger.info(f"[ASIC] Отправлен mining.authorize: {auth_msg}")
                        elif msg.get("method") == "mining.authorize":
                            if hasattr(self.server, 'proxy'):
                                self.server.proxy._send_to_pool(msg)
                                response = {"id": msg["id"], "result": True, "error": None}
                                self.request.sendall((json.dumps(response) + '\n').encode('utf-8'))
                                logger.info(f"[ASIC] Отправлен ответ на mining.authorize: {response}")
                                self.server.proxy.authorized = True
                        elif msg.get("method") == "mining.submit" and hasattr(self.server, 'proxy'):
                            self.server.proxy._send_to_pool(msg)
                            logger.info(f"[ASIC] Шара получена и передана в пул: {msg}")
                        elif hasattr(self.server, 'proxy'):
                            self.server.proxy._send_to_pool(msg)
                    except json.JSONDecodeError:
                        logger.error(f"[ASIC DATA] Некорректный JSON от ASIC: {data}")
                else:
                    logger.warning("[ASIC] Соединение с ASIC разорвано")
                    break
            except Exception as e:
                logger.error(f"[ASIC] Ошибка чтения данных от ASIC: {e}")
                break

class PoolConnector(threading.Thread):
    def __init__(self, server):
        super().__init__(daemon=True)
        self.server = server
        self.asic_socket = None
        self.pool_socket = None
        self.extranonce1 = None
        self.extranonce2_size = 0
        self.job_id = None
        self.difficulty = None
        self.authorized = False
        self.jobs = {}
        self.accepted_shares = 0
        self.rejected_shares = 0
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.cur = self.conn.cursor()
        self._init_db()

    def _init_db(self):
        db_dir = os.path.dirname(DB_PATH)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
        self.cur.execute(""" 
            CREATE TABLE IF NOT EXISTS submits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                extranonce2 TEXT,
                timestamp INTEGER
            )
        """)
        self.conn.commit()

    def run(self):
        while True:
            try:
                with socket.create_connection((POOL_HOST, POOL_PORT)) as s:
                    self.pool_socket = s
                    logger.info(f"[POOL] Подключение к {POOL_HOST}:{POOL_PORT}")
                    self._subscribe()
                    while True:
                        data = self._recv()
                        if data:
                            for message in self._split_messages(data):
                                try:
                                    self._handle_pool_message(json.loads(message))
                                except Exception as e:
                                    logger.error(f"Ошибка обработки сообщения от пула: {e}\n{message}")
                        else:
                            break
            except Exception as e:
                logger.error(f"[POOL] Ошибка в соединении с пулом: {e}")
                time.sleep(5)

    def _recv(self):
        try:
            self.pool_socket.settimeout(30.0)
            buffer = ""
            while True:
                chunk = self.pool_socket.recv(4096).decode('utf-8')
                if not chunk:
                    break
                buffer += chunk
                # Если количество открывающих и закрывающих фигурных скобок совпадает и строка оканчивается \n
                if buffer.count('{') == buffer.count('}') and buffer.endswith('\n'):
                    break
            return buffer
        except socket.timeout:
            logger.warning("[RECV] Таймаут получения данных от пула")
            return ''
        except Exception as e:
            logger.error(f"[RECV] Ошибка получения данных: {e}")
            return ''

    def _split_messages(self, data):
        return data.strip().split('\n')

    def _send_to_pool(self, msg):
        if self.pool_socket:
            message = json.dumps(msg) + '\n'
            self.pool_socket.sendall(message.encode('utf-8'))

    def _send_to_asic(self, msg):
        if self.asic_socket and self.asic_socket.fileno() != -1:
            try:
                message = json.dumps(msg) + '\n'
                self.asic_socket.sendall(message.encode('utf-8'))
                logger.info(f"[SEND] Сообщение отправлено в ASIC: {msg}")
            except Exception as e:
                logger.error(f"[SEND] Ошибка при отправке сообщения в ASIC: {e}")
                self._reconnect_asic()

    def _reconnect_asic(self):
        logger.warning("[RECONNECT] Попытка переподключения к ASIC...")
        try:
            if self.asic_socket and self.asic_socket.fileno() != -1:
                self.asic_socket.close()
            self.asic_socket = None
            while not self.asic_socket:
                if hasattr(self.server, 'asic_socket') and self.server.asic_socket.fileno() != -1:
                    self.asic_socket = self.server.asic_socket
                    logger.info("[RECONNECT] Успешное переподключение к ASIC")
                    break
                time.sleep(1)
        except Exception as e:
            logger.error(f"[RECONNECT] Ошибка при переподключении к ASIC: {e}")

    def _subscribe(self):
        self._send_to_pool({"id": 1, "method": "mining.subscribe", "params": []})
        self._send_to_pool({"id": 2, "method": "mining.authorize", "params": ["worker", "x"]})

    def _handle_pool_message(self, msg):
        logger.debug(f"Сообщение от пула: {msg}")
        if msg.get("method") == "mining.set_difficulty":
            self.difficulty = msg['params'][0]
        elif msg.get("method") == "mining.notify" and self.authorized:
            params = msg.get("params", [])
            if isinstance(params, list) and len(params) >= 8:
                self._handle_notify(params[:8])
            else:
                logger.error(f"Некорректный формат notify: {params}")
        elif msg.get("result") and isinstance(msg['result'], list):
            self.extranonce1 = msg['result'][1]
            self.extranonce2_size = msg['result'][2]
            logger.info(f"[EXTRANONCE1] Получен: {self.extranonce1}, размер: {self.extranonce2_size}")
        elif msg.get("id") == 2 and msg.get("result") is True:
            self.authorized = True
            logger.info("[POOL] Авторизация в пуле успешна")
        # Если сообщение – это ответ на mining.submit
        elif "id" in msg and "method" not in msg and not isinstance(msg.get("result"), list):
            if msg.get("result") is True:
                self.accepted_shares += 1
                logger.info("[SHARE] Принята")
            else:
                self.rejected_shares += 1
                logger.info(f"[SHARE] Отклонена: {msg.get('error')}")

    def _handle_notify(self, params):
        try:
            job_id, coinb1, coinb2, merkle_branch, version, nbits, ntime, clean_jobs = params
            logger.debug(f"[RAW NOTIFY] job_id={job_id}, coinb1={coinb1}, coinb2={coinb2}, merkle_branch={merkle_branch}")
            coinb1 = fix_hex_string(coinb1)
            coinb2 = fix_hex_string(coinb2)
            extranonce2 = self._select_best_extranonce2(job_id)
            extranonce1 = fix_hex_string(self.extranonce1) if self.extranonce1 else "00"
            logger.debug(f"[DEBUG] COINB1: {coinb1}")
            logger.debug(f"[DEBUG] EXTRANONCE1: {extranonce1}, EXTRANONCE2: {extranonce2}")
            logger.debug(f"[DEBUG] COINB2: {coinb2}")

            merkle_root = calculate_merkle_root(coinb1, coinb2, extranonce1, extranonce2, merkle_branch)
            logger.info(f"[FILTERED] job_id={job_id} → best extranonce2: {extranonce2}")
            logger.info(f"[MERKLE ROOT] job_id={job_id} → {merkle_root}")
            logger.info(f"[SEND] Отправлено ASIC: job_id={job_id}")
            self._send_to_asic({
                "id": None,
                "method": "mining.set_difficulty",
                "params": [self.difficulty]
            })
            self._send_to_asic({
                "id": None,
                "method": "mining.notify",
                "params": [job_id, coinb1, coinb2, merkle_branch, version, nbits, ntime, clean_jobs]
            })
        except Exception as e:
            logger.error(f"[HANDLE NOTIFY] Ошибка разбора notify: {e}")

    def _select_best_extranonce2(self, job_id):
        candidates = [f"{i:08x}" for i in range(256)]
        weights = {}
        for ex2 in candidates:
            total = sum(hashlib.sha256(bytes.fromhex(ex2)).digest())
            weights[ex2] = total % (2**32)
        sorted_weights = sorted(weights.items(), key=lambda item: item[1])
        top = sorted_weights[-5:]
        for val, score in top:
            logger.info(f"  - {val} -> вес: {score}")
        best = top[-1][0] if top else "00000000"
        return best

def fix_hex_string(s):
    global last_hex_warning_time, hex_warning_shown
    current_time = time.time()
    if len(s) % 2 != 0:
        if not hex_warning_shown or (current_time - last_hex_warning_time >= 100):
            logger.warning(f"[HEX FIX] Строка нечётной длины, добавляем 0: {s}")
            last_hex_warning_time = current_time
            hex_warning_shown = True
        s = '0' + s
    return s

def calculate_merkle_root(coinb1, coinb2, extranonce1, extranonce2, branches):
    coinb1 = fix_hex_string(coinb1)
    coinb2 = fix_hex_string(coinb2)
    extranonce1 = fix_hex_string(extranonce1)
    extranonce2 = fix_hex_string(extranonce2)
    branches = [fix_hex_string(b) for b in branches]

    coinbase = coinb1 + extranonce1 + extranonce2 + coinb2
    logger.debug(f"[COINBASE RAW] {coinbase}")
    coinbase_bin = binascii.unhexlify(coinbase)
    coinbase_hash = hashlib.sha256(hashlib.sha256(coinbase_bin).digest()).digest()
    merkle_root = coinbase_hash
    for branch in branches:
        merkle_root = hashlib.sha256(hashlib.sha256(merkle_root + binascii.unhexlify(branch)).digest()).digest()
    return merkle_root.hex()

def signal_handler(sig, frame):
    logger.info("[SHUTDOWN] Получен сигнал завершения, закрываем сервер...")
    server.shutdown()
    server.server_close()
    if proxy.pool_socket:
        proxy.pool_socket.close()
    proxy.conn.close()
    os._exit(0)

if __name__ == '__main__':
    ensure_port_available(ASIC_PROXY_PORT)
    server = ThreadedTCPServer((ASIC_PROXY_HOST, ASIC_PROXY_PORT), ASICHandler)
    
    proxy = PoolConnector(server)
    server.proxy = proxy
    proxy.start()
    
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info("[SERVER] Ожидание подключения ASIC...")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Ждём подключения ASIC
    while not hasattr(server, 'asic_socket'):
        time.sleep(1)
    proxy.asic_socket = server.asic_socket
    logger.info("[SERVER] ASIC подключён. Продолжаем работу.")
    logger.info("[MAIN LOGIC] Запуск основного процесса...")

    # Очистка устаревших записей в БД (старше 24 часов)
    cutoff = int(time.time()) - 86400
    with sqlite3.connect(DB_PATH) as db:
        c = db.cursor()
        c.execute("DELETE FROM submits WHERE timestamp < ?", (cutoff,))
        db.commit()

    with open(__file__) as f:
        lines = f.readlines()
        logger.info(f"[FILE] Версия скрипта: {VERSION}, строк: {len(lines)}")

    while True:
        time.sleep(100)
        logger.info(f"[STATS] Accepted: {proxy.accepted_shares}, Rejected: {proxy.rejected_shares}")
