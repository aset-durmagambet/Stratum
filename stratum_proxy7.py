import asyncio
import socket
socket.setdefaulttimeout(60)
import threading
import json
import hashlib
import binascii
import logging
from logging.handlers import RotatingFileHandler
import time
import sqlite3
import os
import signal
import sys
import psutil
import requests
from aiohttp import web
from socketserver import ThreadingMixIn, TCPServer, StreamRequestHandler
from threading import Lock
import aiohttp  # Для асинхронного HTTP в TelegramHandler
POOL_HOST = os.getenv("POOL_HOST", "165.22.46.160")  # Заменили stratum.braiins.com на IP, возвращаемый пингом
POOL_PORT = int(os.getenv("POOL_PORT", 3333))  # Порт остается 3333, это стандарт для Braiins Pool

# Конфигурация прокси для ASIC
ASIC_PROXY_HOST = os.getenv("ASIC_PROXY_HOST", "0.0.0.0")  # Слушаем все интерфейсы, это корректно
ASIC_PROXY_PORT = int(os.getenv("ASIC_PROXY_PORT", 3333))  # Порт для ASIC, совпадает с пулом, что нормально

# Путь к базе данных
DB_PATH = os.getenv("DB_PATH", "asic_proxy.db")  # Оставляем без изменений, проблем с этим нет

# Версия приложения
VERSION = "1.0.0"  # Без изменений

# Конфигурация Telegram для уведомлений
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "7207281851:AAEzDaJmpvA6KB9xgTo7dnEbnW4LUtnH4FQ")  # Токен выглядит валидным
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "480223056")  #

# Инициализация логгера
logger = logging.getLogger("ASICProxy")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler("asic_proxy.log", maxBytes=10*1024*1024, backupCount=5)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# Глобальные переменные для fix_hex_string
last_hex_warning_time = 0
hex_warning_shown = False

# Обработка сигналов для graceful shutdown
def handle_shutdown(signum, frame):
    logger.info("Получен сигнал завершения, останавливаем сервер...")
    if 'server' in globals():
        server.shutdown()
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

async def ensure_port_available(port: int):
    """Освобождает указанный порт, завершая процессы, которые его используют."""
    logger.info(f"[PORT] Проверка порта {port}...")
    processed_pids = set()
    port_in_use_initial = any(p.laddr.port == port for p in psutil.net_connections(kind='inet'))
    if not port_in_use_initial:
        logger.info(f"[PORT] Порт {port} уже свободен.")
        return
    for conn in psutil.net_connections(kind='inet'):
        if conn.laddr.port == port and conn.pid not in processed_pids:
            try:
                proc = psutil.Process(conn.pid)
                logger.info(f"[PORT IN USE] Убиваем процесс PID={conn.pid}, использующий порт :{port}")
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except psutil.TimeoutExpired:
                    logger.warning(f"[PORT] Процесс PID={conn.pid} не завершился, принудительно убиваем...")
                    proc.kill()
                processed_pids.add(conn.pid)
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Ошибка при завершении процесса: {e}")
    timeout = 60
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout:
            logger.error(f"[PORT] Не удалось освободить порт {port} за {timeout} секунд")
            raise RuntimeError(f"Не удалось освободить порт {port}")
        port_in_use = any(p.laddr.port == port for p in psutil.net_connections(kind='inet'))
        if not port_in_use:
            logger.info(f"[PORT] Порт {port} освободился.")
            break
        await asyncio.sleep(1)

class TelegramHandler(logging.Handler):
    """
    Асинхронный обработчик логов, отправляющий сообщения в Телеграм.
    Использует aiohttp для асинхронной отправки, интегрируясь с основным event loop.
    """
    def __init__(self, token, chat_id, loop=None):
        super().__init__()
        self.token = token
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.enabled = True
        self.shutdown_flag = False
        # Используем переданный loop, либо получаем текущий
        self.loop = loop  

    async def send_message(self, message):
        """Асинхронная отправка сообщения в Телеграм."""
        async with aiohttp.ClientSession() as session:
            payload = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            try:
                async with session.post(self.url, data=payload, timeout=10) as resp:
                    await resp.text()
            except Exception as e:
                print(f"[TelegramHandler] Ошибка при отправке в Telegram: {e}")

    def send_response(self, message):
        """
        Планирует отправку сообщения через асинхронную функцию.
        Вызывается синхронно, но работа выполняется в основном цикле событий.
        """
        asyncio.run_coroutine_threadsafe(self.send_message(message), self.loop)

    def handle_command(self, text):
        """
        Обработка команд, полученных, например, от пользователя.
        Доступные команды: /shutdown, /disable, /enable, /status, /help.
        """
        cmd = text.strip().lower()
        if cmd == "/shutdown":
            self.send_response("🛑 Прокси завершает работу по команде /shutdown")
            self.disable()
            logging.warning("[TELEGRAM] Получена команда /shutdown, завершаем прокси...")
            sys.exit(0)
        elif cmd == "/disable":
            self.disable()
            logging.info("[TELEGRAM] Получена команда /disable — логирование отключено.")
            self.send_response("📴 Telegram логирование отключено.")
        elif cmd == "/enable":
            self.enable()
            logging.info("[TELEGRAM] Получена команда /enable — логирование включено.")
            self.send_response("✅ Telegram логирование включено.")
        elif cmd == "/status":
            # Предполагается, что глобальная переменная proxy уже определена.
            accepted = proxy.accepted_shares if 'proxy' in globals() else 0
            rejected = proxy.rejected_shares if 'proxy' in globals() else 0
            msg = f"📊 Шары: принятых {accepted}, отклонённых {rejected}"
            logging.info("[TELEGRAM] Команда /status — %s" % msg)
            self.send_response(msg)
        elif cmd == "/help":
            self.send_response("🧾 Команды:\n/status — статистика\n/enable — включить лог\n/disable — отключить лог\n/shutdown — завершить работу")

    def emit(self, record):
        """
        Переопределенный метод emit для отправки уведомлений о логах.
        Отправляются сообщения, содержащие ключевые слова, такие как 'submit', 'notify', 'asic', 'pool', 'share', 'error'.
        """
        if self.shutdown_flag or not self.enabled:
            return
        try:
            log_entry = self.format(record)
            keywords = ["submit", "notify", "asic", "pool", "share", "error"]
            if any(keyword in log_entry.lower() for keyword in keywords):
                message = f"🚨 <b>{record.levelname}</b>\n<code>{log_entry}</code>"
                asyncio.run_coroutine_threadsafe(self.send_message(message), self.loop)
        except Exception as e:
            print(f"[TelegramHandler] Ошибка при отправке в Telegram: {e}")

    def disable(self):
        """Отключает отправку сообщений через Телеграм."""
        self.shutdown_flag = True
        self.enabled = False

    def enable(self):
        """Включает отправку сообщений через Телеграм."""
        self.enabled = True
        self.shutdown_flag = False

class ThreadedTCPServer(ThreadingMixIn, TCPServer):
    allow_reuse_address = True

class ASICHandler(StreamRequestHandler):
    """Обработчик подключений от ASIC-устройств."""
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
                            while not (hasattr(self.server, 'proxy') and self.server.proxy.extranonce1 is not None and self.server.proxy.extranonce2_size):
                                time.sleep(1)
                            response = {
                                "id": msg["id"],
                                "result": [["mining.notify", "ae6812eb4cd7735a"]],
                                "extranonce1": self.server.proxy.extranonce1,
                                "extranonce2_size": self.server.proxy.extranonce2_size
                            }
                            self.request.sendall((json.dumps(response) + '\n').encode('utf-8'))
                            logger.info(f"[ASIC] Отправлен ответ на mining.subscribe: {response}")
                            auth_msg = {
                                "id": msg["id"] + 1,
                                "method": "mining.authorize",
                                "params": ["worker", "x"]
                            }
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
    """Соединитель с майнинг-пулом."""
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
        self.db_lock = Lock()  # Блокировка для SQLite
        db_dir = os.path.dirname(DB_PATH)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.cur = self.conn.cursor()
        self._init_db()

    def _init_db(self):
        with self.db_lock:
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
                logger.info(f"[POOL] Попытка подключения к пулу {POOL_HOST}:{POOL_PORT}...")  # Лог подключения
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
        if not self.pool_socket or self.pool_socket.fileno() == -1:
            logger.warning("[POOL] Соединение с пулом потеряно, переподключаемся...")
            self.pool_socket = None
            return
        try:
            message = json.dumps(msg) + '\n'
            self.pool_socket.sendall(message.encode('utf-8'))
            logger.debug(f"[POOL SEND] Отправлено в пул: {msg}")
        except Exception as e:
            logger.error(f"[POOL SEND] Ошибка отправки: {e}, переподключаемся...")
            self.pool_socket = None

    def _send_to_asic(self, msg):
        if self.asic_socket and self.asic_socket.fileno() != -1:
            try:
                message = json.dumps(msg) + '\n'
                self.asic_socket.sendall(message.encode('utf-8'))
                logger.info(f"[SEND] Сообщение отправлено в ASIC: {msg}")
            except Exception as e:
                logger.error(f"[SEND] Ошибка при отправке сообщения в ASIC: {e}")
                self._reconnect_asic()
        else:
            logger.warning("[SEND] ASIC-сокет недоступен, пытаемся переподключиться...")
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
    """Исправляет шестнадцатеричную строку, добавляя ведущий ноль при необходимости."""
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
    """Вычисляет корень Меркла для майнинга."""
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

async def main_async():
    global server, proxy, telegram_handler
    logger.info("[STARTUP] Программа начала выполнение...")
    try:
        # Освобождение порта и запуск TCP-сервера
        logger.info(f"[PORT] Проверка порта {ASIC_PROXY_PORT}...")
        await ensure_port_available(ASIC_PROXY_PORT)
        logger.info(f"[PORT] Порт {ASIC_PROXY_PORT} доступен.")
        logger.info(f"[SERVER] Запуск TCP-сервера на {ASIC_PROXY_HOST}:{ASIC_PROXY_PORT}...")
        server = ThreadedTCPServer((ASIC_PROXY_HOST, ASIC_PROXY_PORT), ASICHandler)
        proxy = PoolConnector(server)
        server.proxy = proxy
        proxy.start()
        threading.Thread(target=server.serve_forever, daemon=True).start()
        logger.info(f"[SERVER] Прокси-сервер запущен на {ASIC_PROXY_HOST}:{ASIC_PROXY_PORT}")

        # Настройка HTTP-мониторинга
        async def handle_status(request):
            return web.json_response({
                "version": VERSION,
                "accepted_shares": proxy.accepted_shares,
                "rejected_shares": proxy.rejected_shares,
                "connected": proxy.pool_socket is not None
            })

        async def handle_shutdown(request):
            telegram_handler.disable()
            logger.info("[TELEGRAM] Отключено через /shutdown")
            sys.exit(0)  
           
        app = web.Application()
        app.add_routes([
            web.get('/status', handle_status),
            web.get('/shutdown', handle_shutdown)
        ])
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()
        logger.info("[HTTP] Мониторинг доступен на порту 8080")

        # Инициализация Telegram-логирования с использованием текущего event loop
        current_loop = asyncio.get_running_loop()
        telegram_handler = TelegramHandler(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, loop=current_loop)
        telegram_handler.setLevel(logging.DEBUG)
        telegram_handler.setFormatter(formatter)
        logger.addHandler(telegram_handler)

        # Бесконечный цикл для поддержания работы приложения
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        logger.error(f"[MAIN] Ошибка в main_async: {e}")
        raise
    finally:
        logger.info("Закрываем ресурсы...")
        if 'proxy' in globals() and proxy.conn:
            proxy.conn.close()
        if 'server' in globals():
            server.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.warning("🛑 Остановка по Ctrl+C")
        try:
            telegram_handler.disable()  # Отключение Telegram-логирования при остановке
            logger.info("[TELEGRAM] Логирование отключено при остановке.")
            server.shutdown()
            logger.info("[SERVER] Сервер корректно остановлен.")
        except Exception as e:
            logger.error(f"[ERROR] При остановке сервера: {e}")
        sys.exit(0)
        

        
        
        
        
