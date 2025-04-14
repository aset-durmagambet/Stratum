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
from logging.handlers import RotatingFileHandler

# === Глобальные переменные и настройки ===
ASIC_PROXY_HOST = '0.0.0.0'
ASIC_PROXY_PORT = 3333
POOL_HOST = 'stratum.braiins.com'
POOL_PORT = 3333
VERSION = "1.0.0"

TELEGRAM_TOKEN = '7207281851:AAEzDaJmpvA6KB9xgTo7dnEbnW4LUtnH4FQ'
TELEGRAM_CHAT_ID = 480223056

LOG_DIR = "/root/Stratum/logs"
LOG_FILE = os.path.join(LOG_DIR, "stratum_proxy.log")
ERROR_LOG_FILE = os.path.join(LOG_DIR, "stratum_proxy_error.log")
DB_PATH = "/root/Stratum/submits.db"

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5)
file_handler.setFormatter(file_formatter)

error_handler = RotatingFileHandler(ERROR_LOG_FILE, maxBytes=5*1024*1024, backupCount=3)
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(file_formatter)

logger = logging.getLogger("stratum_proxy")
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(error_handler)
logger.addHandler(console_handler)

import logging

# === Обработчик Telegram ===
class TelegramHandler(logging.Handler):
    def __init__(self, token, chat_id):
        super().__init__()
        self.token = token
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{self.token}/sendMessage"

    def emit(self, record):
        try:
            msg = self.format(record)
            safe_msg = self._sanitize_text(msg)
            self._send(safe_msg)
        except Exception as e:
            print(f"[TelegramHandler] Ошибка при отправке: {e}")

    def _send(self, text):
        try:
            response = requests.post(self.url, data={
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML"
            }, timeout=5)
            if response.status_code != 200:
                print(f"[TelegramHandler] Ответ Telegram: {response.status_code} — {response.text}")
        except Exception as e:
            print(f"[TelegramHandler] Ошибка сети: {e}")

    def _sanitize_text(self, text):
        return text.encode("utf-8", "ignore").decode("utf-8", "ignore")

# === Обработчик ASIC ===
class ASICHandler(StreamRequestHandler):
    def handle(self):
        logger.info(f"[ASIC] Подключение от {self.client_address[0]}")
        proxy.asic_socket = self.request
        asic_status["connected"] = True

        while True:
            try:
                data = self.request.recv(4096).decode('utf-8', errors='replace')
                if not data:
                    logger.warning("[ASIC] Соединение разорвано")
                    asic_status["connected"] = False
                    break

                asic_status["last_message_time"] = time.strftime('%Y-%m-%d %H:%M:%S')
                asic_status["messages_received"] += 1

                try:
                    msg = json.loads(data)
                    method = msg.get("method")
                    asic_status["last_method"] = method

                    if method == "mining.submit":
                        proxy.pool_socket.sendall((json.dumps(msg) + "\n").encode())
                        logger.info(f"[SUBMIT] От ASIC: {msg}")

                        job_id = msg.get("params", [None, None])[1]
                        extranonce2 = msg.get("params", [None, None, None])[2]
                        timestamp = int(time.time())
                        try:
                            proxy.cur.execute(
                                "INSERT INTO submits (job_id, extranonce2, timestamp, status) VALUES (?, ?, ?, ?)",
                                (job_id, extranonce2, timestamp, 'submitted')
                            )
                            proxy.conn.commit()
                            proxy.last_submit_id = proxy.cur.lastrowid
                        except Exception as e:
                            logger.error(f"[DB] Ошибка записи submit: {e}")
                except Exception as e:
                    logger.error(f"[ASIC] Ошибка разбора JSON: {e}")
                    asic_status["last_error"] = str(e)
            except Exception as e:
                logger.error(f"[ASIC] Ошибка чтения: {e}")
                asic_status["connected"] = False
                asic_status["last_error"] = str(e)
                break

# Пример создания логгера с использованием TelegramHandler
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Создаём экземпляр обработчика Telegram
telegram_handler = TelegramHandler(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
telegram_handler.setLevel(logging.INFO)

formatter = logging.Formatter('[%(levelname)s] %(message)s')
telegram_handler.setFormatter(formatter)

# Добавляем обработчик в логгер
logger.addHandler(telegram_handler)

# Теперь можно запускать другие части кода


# === Telegram polling-команды ===
def poll_telegram_commands():
    last_update_id = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates?offset={last_update_id + 1}"
            response = requests.get(url, timeout=5)
            data = response.json()
            for result in data.get("result", []):
                message = result.get("message")
                if not message:
                    continue
                chat_id = message.get("chat", {}).get("id")
                if chat_id != TELEGRAM_CHAT_ID:
                    continue
                text = message.get("text", "").strip().lower()
                if text == "/status":
                    status_lines = [
                        "🟢 ASIC подключён" if asic_status["connected"] else "🔴 ASIC отключён",
                        f"📨 Получено: {asic_status['messages_received']}",
                        f"🕒 Последнее: {asic_status['last_message_time'] or '—'}",
                        f"🧠 Метод: {asic_status['last_method'] or '—'}",
                        f"🔥 Ошибка: {asic_status['last_error'] or 'Нет'}",
                        f"🔔 Уведомления: {'включены' if asic_status['alerts_enabled'] else 'выключены'}",
                        f"✅ Принято: {getattr(proxy, 'accepted_shares', 0)} | ❌ Отклонено: {getattr(proxy, 'rejected_shares', 0)}"
                    ]
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": "\n".join(status_lines)
                    })
                elif text == "/alerts_off":
                    asic_status["alerts_enabled"] = False
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": "🔕 Уведомления отключены."
                    })
                elif text == "/alerts_on":
                    asic_status["alerts_enabled"] = True
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": "🔔 Уведомления включены."
                    })
                elif text == "/restart_asic":
                    try:
                        if proxy.asic_socket:
                            proxy.asic_socket.close()
                            proxy.asic_socket = None
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
                            "chat_id": TELEGRAM_CHAT_ID,
                            "text": "♻️ Перезапуск соединения с ASIC инициирован."
                        })
                    except Exception as e:
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
                            "chat_id": TELEGRAM_CHAT_ID,
                            "text": f"❌ Ошибка при перезапуске ASIC: {e}"
                        })
                elif text == "/last_submit":
                    try:
                        proxy.cur.execute("SELECT job_id, extranonce2, timestamp, status FROM submits ORDER BY id DESC LIMIT 1")
                        row = proxy.cur.fetchone()
                        if row:
                            msg = (
                                f"📝 Последний submit:\n"
                                f"Job ID: <code>{row[0]}</code>\n"
                                f"Extranonce2: <code>{row[1]}</code>\n"
                                f"Status: <b>{row[3]}</b>\n"
                                f"⏱ Время: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(row[2]))}"
                            )
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
                                "chat_id": TELEGRAM_CHAT_ID,
                                "text": msg,
                                "parse_mode": "HTML"
                            })
                        else:
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
                                "chat_id": TELEGRAM_CHAT_ID,
                                "text": "❗ Нет данных о submit'ах"
                            })
                    except Exception as e:
                        logger.error(f"[TELEGRAM] Ошибка обработки /last_submit: {e}")
                last_update_id = result.get("update_id", last_update_id)
        except Exception as e:
            logger.warning(f"[TELEGRAM POLL] Ошибка: {e}")
        time.sleep(3)


threading.Thread(target=poll_telegram_commands, daemon=True).start()

# === Глобальный статус ===
asic_status = {
    "connected": False,
    "last_message_time": None,
    "last_method": None,
    "messages_received": 0,
    "last_error": None,
    "alerts_enabled": True
}

# === Мониторинг статуса ASIC и системы ===
def asic_status_watcher():
    prev_connected = None
    last_alert_time = 0
    alert_interval = 120

    while True:
        time.sleep(30)

        now_str = time.strftime('%Y-%m-%d %H:%M:%S')
        now_ts = time.time()
        last_msg_time_str = asic_status.get("last_message_time")
        last_msg_ts = None

        try:
            last_msg_ts = time.mktime(time.strptime(last_msg_time_str, '%Y-%m-%d %H:%M:%S'))
        except:
            pass

        connected = asic_status.get("connected", False)
        alerts_on = asic_status.get("alerts_enabled", True)

        if alerts_on:
            if prev_connected is True and connected is False:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": f"🔴 ASIC отключился в {now_str}"
                })
                logger.warning("[ALERT] ASIC отключился")

            if prev_connected is False and connected is True:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": f"🟢 ASIC снова подключён в {now_str}"
                })
                logger.info("[ALERT] ASIC восстановил соединение")

            if connected and last_msg_ts and now_ts - last_msg_ts > alert_interval:
                if now_ts - last_alert_time > alert_interval:
                    last_alert_time = now_ts
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": f"⏱ ASIC не отправлял сообщений более {(now_ts - last_msg_ts)//60:.0f} мин (last: {last_msg_time_str})"
                    })
                    logger.warning("[ALERT] ASIC не отправлял данные")

        prev_connected = connected

threading.Thread(target=asic_status_watcher, daemon=True).start()
asic_status = {
    "connected": False,
    "last_message_time": None,
    "last_method": None,
    "messages_received": 0,
    "last_error": None
}

# === Вспомогательные функции ===
def fix_hex_string(s):
    return '0' + s if len(s) % 2 else s

def calculate_merkle_root(coinb1, coinb2, extranonce1, extranonce2, branches):
    coinbase = binascii.unhexlify(coinb1 + extranonce1 + extranonce2 + coinb2)
    coinbase_hash = hashlib.sha256(hashlib.sha256(coinbase).digest()).digest()
    merkle_root = coinbase_hash
    for branch in branches:
        merkle_root = hashlib.sha256(hashlib.sha256(merkle_root + binascii.unhexlify(fix_hex_string(branch))).digest()).digest()
    return merkle_root[::-1].hex()

def select_best_extranonce2(size):
    candidates = [f"{i:0{size * 2}x}" for i in range(256)]
    weights = {val: sum(hashlib.sha256(bytes.fromhex(val)).digest()) for val in candidates}
    return sorted(weights.items(), key=lambda x: x[1])[-1][0]

# === Прокси-заглушка ===
class DummyProxy:
    def __init__(self):
        self.asic_socket = None
        self.pool_socket = None
        self.accepted_shares = 0
        self.rejected_shares = 0
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.cur = self.conn.cursor()
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS submits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                extranonce2 TEXT,
                timestamp INTEGER,
                status TEXT
            )
        """)
        self.conn.commit()
        self.last_submit_id = None

proxy = DummyProxy()

# === Обработчик ASIC ===
class TelegramHandler(logging.Handler):
    def __init__(self, token, chat_id):
        super().__init__()
        self.token = token
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{self.token}/sendMessage"

    def emit(self, record):
        try:
            msg = self.format(record)
            
            # Если это не критичная ошибка (например, ошибка разрыва соединения), не отправлять в Telegram
            if "[ERROR] Ошибка чтения: [Errno 104]" in msg:
                return  # Игнорируем такие ошибки

            safe_msg = self._sanitize_text(msg)
            self._send(safe_msg)
        except Exception as e:
            print(f"[TelegramHandler] Ошибка при отправке: {e}")

    def _send(self, text):
        try:
            response = requests.post(self.url, data={
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML"
            }, timeout=5)
            if response.status_code != 200:
                print(f"[TelegramHandler] Ответ Telegram: {response.status_code} — {response.text}")
        except Exception as e:
            print(f"[TelegramHandler] Ошибка сети: {e}")

    def _sanitize_text(self, text):
        return text.encode("utf-8", "ignore").decode("utf-8", "ignore")
    def handle(self):
        logger.info(f"[ASIC] Подключение от {self.client_address[0]}")
        proxy.asic_socket = self.request
        asic_status["connected"] = True

        while True:
            try:
                data = self.request.recv(4096).decode('utf-8', errors='replace')
                if not data:
                    logger.warning("[ASIC] Соединение разорвано")
                    asic_status["connected"] = False
                    break

                asic_status["last_message_time"] = time.strftime('%Y-%m-%d %H:%M:%S')
                asic_status["messages_received"] += 1

                try:
                    msg = json.loads(data)
                    method = msg.get("method")
                    asic_status["last_method"] = method

                    if method == "mining.submit":
                        proxy.pool_socket.sendall((json.dumps(msg) + "\n").encode())
                        logger.info(f"[SUBMIT] От ASIC: {msg}")

                        job_id = msg.get("params", [None, None])[1]
                        extranonce2 = msg.get("params", [None, None, None])[2]
                        timestamp = int(time.time())
                        try:
                            proxy.cur.execute(
                                "INSERT INTO submits (job_id, extranonce2, timestamp, status) VALUES (?, ?, ?, ?)",
                                (job_id, extranonce2, timestamp, 'submitted')
                            )
                            proxy.conn.commit()
                            proxy.last_submit_id = proxy.cur.lastrowid
                        except Exception as e:
                            logger.error(f"[DB] Ошибка записи submit: {e}")
                except Exception as e:
                    logger.error(f"[ASIC] Ошибка разбора JSON: {e}")
                    asic_status["last_error"] = str(e)
            except ConnectionResetError as e:
                logger.warning(f"[ASIC] Ошибка соединения: {e} — Пытаемся переподключиться...")
                time.sleep(5)  # Ждём 5 секунд перед новой попыткой
                try:
                    self.request.close()  # Закрываем текущее соединение
                    proxy.asic_socket = None  # Обнуляем сокет
                    # Можно добавить повторное подключение к ASIC или пулу
                except Exception as close_e:
                    logger.error(f"[ASIC] Ошибка закрытия сокета: {close_e}")
            except Exception as e:
                logger.error(f"[ASIC] Ошибка чтения: {e}")
                asic_status["connected"] = False
                asic_status["last_error"] = str(e)
                break
    def handle(self):
        logger.info(f"[ASIC] Подключение от {self.client_address[0]}")
        proxy.asic_socket = self.request
        asic_status["connected"] = True

        while True:
            try:
                data = self.request.recv(4096).decode('utf-8', errors='replace')
                if not data:
                    logger.warning("[ASIC] Соединение разорвано")
                    asic_status["connected"] = False
                    break

                asic_status["last_message_time"] = time.strftime('%Y-%m-%d %H:%M:%S')
                asic_status["messages_received"] += 1

                try:
                    msg = json.loads(data)
                    method = msg.get("method")
                    asic_status["last_method"] = method

                    if method == "mining.submit":
                        proxy.pool_socket.sendall((json.dumps(msg) + "\n").encode())
                        logger.info(f"[SUBMIT] От ASIC: {msg}")

                        job_id = msg.get("params", [None, None])[1]
                        extranonce2 = msg.get("params", [None, None, None])[2]
                        timestamp = int(time.time())
                        try:
                            proxy.cur.execute(
                                "INSERT INTO submits (job_id, extranonce2, timestamp, status) VALUES (?, ?, ?, ?)",
                                (job_id, extranonce2, timestamp, 'submitted')
                            )
                            proxy.conn.commit()
                            proxy.last_submit_id = proxy.cur.lastrowid
                        except Exception as e:
                            logger.error(f"[DB] Ошибка записи submit: {e}")
                except Exception as e:
                    logger.error(f"[ASIC] Ошибка разбора JSON: {e}")
                    asic_status["last_error"] = str(e)
            except Exception as e:
                logger.error(f"[ASIC] Ошибка чтения: {e}")
                asic_status["connected"] = False
                asic_status["last_error"] = str(e)
                break


# === PoolConnector ===
class PoolConnector(threading.Thread):
    def _split_messages(self, data):
        return [msg for msg in data.strip().split('\n') if msg]

    def __init__(self):
        super().__init__(daemon=True)
        self.extranonce1 = None
        self.extranonce2_size = 0
        self.difficulty = None
        self.authorized = False

    def run(self):
        while True:
            try:
                with socket.create_connection((POOL_HOST, POOL_PORT)) as s:
                    proxy.pool_socket = s
                    logger.info(f"[POOL] Подключен к {POOL_HOST}:{POOL_PORT}")
                    self._subscribe()
                    while True:
                        buffer = ""
                        while True:
                            chunk = s.recv(4096).decode('utf-8')
                            if not chunk:
                                raise ConnectionError("Соединение разорвано пулом")
                            buffer += chunk
                            if buffer.endswith('\n'):
                                break
                        for line in self._split_messages(buffer):
                            self._handle_pool_message(json.loads(line))
                    while True:
                        data = s.recv(4096).decode('utf-8')
                        buffer = ""
                        while True:
                            chunk = s.recv(4096).decode('utf-8')
                            if not chunk:
                                break
                            buffer += chunk
                            if buffer.endswith('\n'):
                                break
                        lines = buffer.strip().split('\n')
                        for line in lines:
                            self._handle_pool_message(json.loads(line))
            except Exception as e:
                logger.error(f"[POOL] Ошибка: {e}")
                time.sleep(5)

    def _subscribe(self):
        self._send({"id": 1, "method": "mining.subscribe", "params": []})
        self._send({"id": 2, "method": "mining.authorize", "params": ["worker", "x"]})

    def _send(self, msg):
        if proxy.pool_socket:
            proxy.pool_socket.sendall((json.dumps(msg) + '\n').encode())

    def _send_to_asic(self, msg):
     if proxy.asic_socket:
        try:
            proxy.asic_socket.sendall((json.dumps(msg) + "\n").encode())
        except Exception as e:
            logger.error(f"[SEND ASIC] Ошибка: {e}")

    def _handle_pool_message(self, msg):
        if msg.get("method") == "mining.set_difficulty":
            self.difficulty = msg['params'][0]
        elif msg.get("method") == "mining.notify" and self.authorized:
            params = msg.get("params", [])
            if len(params) >= 8:
                job_id, coinb1, coinb2, merkle_branch, version, nbits, ntime, clean_jobs = params[:8]
                extranonce2 = select_best_extranonce2(self.extranonce2_size)
                merkle_root = calculate_merkle_root(coinb1, coinb2, self.extranonce1, extranonce2, merkle_branch)
                logger.info(f"[FILTER] extranonce2: {extranonce2}, Merkle: {merkle_root}")
                self._send_to_asic({"id": None, "method": "mining.set_difficulty", "params": [self.difficulty]})
                self._send_to_asic({"id": None, "method": "mining.notify", "params": params})
        elif msg.get("result") and isinstance(msg['result'], list):
            self.extranonce1 = msg['result'][1]
            self.extranonce2_size = msg['result'][2]
            logger.info(f"[POOL] extranonce1: {self.extranonce1}, size: {self.extranonce2_size}")
        elif msg.get("id") == 2 and msg.get("result") is True:
            self.authorized = True
            logger.info("[POOL] Авторизация успешна")
        elif msg.get("id") and "method" not in msg:
            status = 'accepted' if msg.get("result") is True else 'rejected'
            try:
                if proxy.last_submit_id:
                    proxy.cur.execute("UPDATE submits SET status = ? WHERE id = ?", (status, proxy.last_submit_id))
                    proxy.conn.commit()
                    if status == 'accepted':
                        proxy.accepted_shares += 1
                    else:
                        proxy.rejected_shares += 1
            except Exception as e:
                logger.error(f"[DB] Ошибка обновления submit: {e}")

# === REST API ===
class RESTHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(self.path)
        token = parse_qs(parsed.query).get('token', [''])[0]
        if token != 'admin123':
            self.send_response(403)
            self.end_headers()
            return

        if parsed.path == '/api/status':
            self._respond_json(asic_status)
        elif parsed.path == '/api/logs':
            with sqlite3.connect(DB_PATH) as db:
                cur = db.cursor()
                cur.execute("SELECT id, job_id, extranonce2, timestamp, status FROM submits ORDER BY id DESC LIMIT 100")
                logs = [dict(id=r[0], job_id=r[1], extranonce2=r[2], timestamp=r[3], status=r[4]) for r in cur.fetchall()]
                self._respond_json(logs)
        elif parsed.path == '/':
            html = f"<html><body><h2>ASIC: {'🟢' if asic_status['connected'] else '🔴'}</h2></body></html>"
            self._respond_html(html)
        else:
            self.send_response(404)
            self.end_headers()

    def _respond_json(self, obj):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(obj).encode())

    def _respond_html(self, html):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(html.encode())

# === Проверка порта ===
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
    while any(p.laddr.port == port for p in psutil.net_connections()):
        time.sleep(1)

# === Ожидание подключения ASIC ===
def wait_for_asic():
    while proxy.asic_socket is None:
        logger.info("[WAIT] Ожидание подключения ASIC...")
        time.sleep(5)
    logger.info("[WAIT] ASIC подключён!")

# === Сигналы и запуск ===
def signal_handler(sig, frame):
    logger.info("[SHUTDOWN] Завершение по сигналу")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == '__main__':
    ensure_port_available(ASIC_PROXY_PORT)
    api_server = HTTPServer(('0.0.0.0', 8080), RESTHandler)
    threading.Thread(target=api_server.serve_forever, daemon=True).start()
    logger.info("[SERVER] API запущен на порту 8080")

    proxy_server = TCPServer((ASIC_PROXY_HOST, ASIC_PROXY_PORT), ASICHandler)
    threading.Thread(target=proxy_server.serve_forever, daemon=True).start()
    logger.info("[SERVER] Старт Stratum proxy")

    pool_thread = PoolConnector()
    pool_thread.start()

    threading.Thread(target=wait_for_asic, daemon=True).start()

    while True:
        time.sleep(60)
