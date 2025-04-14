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
import requests
import sys

from socketserver import ThreadingMixIn, TCPServer, StreamRequestHandler
from http.server import BaseHTTPRequestHandler, HTTPServer

ASIC_PROXY_HOST = '0.0.0.0'
ASIC_PROXY_PORT = 3333
POOL_HOST = 'stratum.braiins.com'
POOL_PORT = 3333
VERSION = "1.0.0"

# Настройка логирования с TelegramHandler
from logging.handlers import RotatingFileHandler

LOG_DIR = "/root/Stratum/logs"
LOG_FILE = os.path.join(LOG_DIR, "stratum_proxy.log")
ERROR_LOG_FILE = os.path.join(LOG_DIR, "stratum_proxy_error.log")

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(file_formatter)

error_handler = RotatingFileHandler(ERROR_LOG_FILE, maxBytes=5*1024*1024, backupCount=3)
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(file_formatter)

class TelegramHandler(logging.Handler):
    def __init__(self, token, chat_id):
        super().__init__()
        self.token = token
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.enabled = True

    def emit(self, record):
        if not self.enabled:
            return
        try:
            log_entry = self.format(record)
            payload = {
                "chat_id": self.chat_id,
                "text": f"\ud83d\udea8 <b>{record.levelname}</b>\n<pre>{log_entry}</pre>",
                "parse_mode": "HTML"
            }
            requests.post(self.url, data=payload, timeout=10)
        except Exception as e:
            print(f"[TelegramHandler] Ошибка при отправке в Telegram: {e}")

    def disable(self):
        self.enabled = False

    def enable(self):
        self.enabled = True

TELEGRAM_TOKEN = '7207281851:AAEzDaJmpvA6KB9xgTo7dnEbnW4LUtnH4FQ'
TELEGRAM_CHAT_ID = 480223056

telegram_handler = TelegramHandler(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
telegram_handler.setLevel(logging.ERROR)
telegram_handler.setFormatter(file_formatter)

logger = logging.getLogger("stratum_proxy")
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(error_handler)
logger.addHandler(console_handler)
logger.addHandler(telegram_handler)

DB_PATH = "/root/Stratum/submits.db"

# Глобальные переменные для контроля предупреждений по HEX
last_hex_warning_time = 0
hex_warning_shown = False

# Обработка входящих Telegram-команд
import threading

def poll_telegram_commands():
    last_update_id = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates?offset={last_update_id + 1}"
            response = requests.get(url, timeout=10)
            data = response.json()
            for result in data.get("result", []):
                message = result.get("message")
                if not message:
                    continue
                chat_id = message.get("chat", {}).get("id")
                if chat_id != TELEGRAM_CHAT_ID:
                    continue
                text = message.get("text", "").strip().lower()
                if text == "/shutdown":
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": "\u274c Остановка прокси по команде /shutdown",
                    })
                    os._exit(0)
                elif text == "/disable":
                    telegram_handler.disable()
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": "\u26d4 Telegram-уведомления отключены."
                    })
                elif text == "/enable":
                    telegram_handler.enable()
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": "\u2705 Telegram-уведомления включены."
                    })
                elif text == "/status":
                    status = f"✅ Принято: {getattr(proxy, 'accepted_shares', 0)} | ❌ Отклонено: {getattr(proxy, 'rejected_shares', 0)}"
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": status
                    })
                last_update_id = result.get("update_id", last_update_id)
        except Exception as e:
            logger.warning(f"[TELEGRAM POLL] Ошибка: {e}")
        time.sleep(3)

threading.Thread(target=poll_telegram_commands, daemon=True).start()

# HTTP API для управления ботом
class CommandHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/disable_telegram':
            telegram_handler.disable()
            self._respond(200, "Telegram notifications disabled.")
        elif self.path == '/enable_telegram':
            telegram_handler.enable()
            self._respond(200, "Telegram notifications enabled.")
        elif self.path == '/clear_logs':
            open(LOG_FILE, 'w').close()
            open(ERROR_LOG_FILE, 'w').close()
            self._respond(200, "Logs cleared.")
        elif self.path == '/shutdown':
            self._respond(200, "Shutting down...")
            threading.Thread(target=self._shutdown_server, daemon=True).start()
        else:
            self._respond(404, "Unknown command")

    def _respond(self, code, message):
        self.send_response(code)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(message.encode())

    def _shutdown_server(self):
        logger.info("[HTTP API] Получена команда /shutdown")
        time.sleep(1)
        os._exit(0)

# Запуск HTTP API-сервера на 8081 порту в фоне
http_server = HTTPServer(('0.0.0.0', 8181), CommandHandler)
th_api = threading.Thread(target=http_server.serve_forever, daemon=True)
th_api.start()


# HTTP API для управления ботом
class CommandHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/disable_telegram':
            telegram_handler.disable()
            self._respond(200, "Telegram notifications disabled.")
        elif self.path == '/enable_telegram':
            telegram_handler.enable()
            self._respond(200, "Telegram notifications enabled.")
        elif self.path == '/clear_logs':
            open(LOG_FILE, 'w').close()
            open(ERROR_LOG_FILE, 'w').close()
            self._respond(200, "Logs cleared.")
        elif self.path == '/shutdown':
            self._respond(200, "Shutting down...")
            threading.Thread(target=self._shutdown_server, daemon=True).start()
        else:
            self._respond(404, "Unknown command")

    def _respond(self, code, message):
        self.send_response(code)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(message.encode())

    def _shutdown_server(self):
        logger.info("[HTTP API] Получена команда /shutdown")
        time.sleep(1)
        os._exit(0)

# Запуск HTTP API-сервера на 8080 порту в фоне
http_server = HTTPServer(('0.0.0.0', 8080), CommandHandler)
th_api = threading.Thread(target=http_server.serve_forever, daemon=True)
th_api.start()

# Остальной код продолжается без изменений...
# Ты можешь скопировать всё, начиная с def ensure_port_available(...) и до конца, из исходного кода.


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
        # Обработка ответа на mining.submit
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

    # Основной цикл: каждые 100 секунд выводим статистику по шарам
    while True:
        time.sleep(100)
        logger.info(f"[SHARES STATS] Принято: {proxy.accepted_shares} | Отклонено: {proxy.rejected_shares}")
