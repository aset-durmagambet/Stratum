from dotenv import load_dotenv
load_dotenv()
import asyncio
import socket
import threading
from threading import Thread
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
import aiohttp
from aiohttp import web
from socketserver import ThreadingMixIn, TCPServer, StreamRequestHandler
from threading import Lock
from contextlib import contextmanager

# Вывод текущей директории и содержимого
print("✅ Текущая директория:", os.getcwd())
print("📄 Содержимое:", os.listdir())

LOG_FILE = "/root/Stratum/asic_proxy.log"

# Создаём директорию для логов
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# Очистка лог-файла при запуске
with open(LOG_FILE, "w") as f:
    f.write("")

# Инициализация логгера
logger = logging.getLogger("ASICProxy")
logger.setLevel(logging.DEBUG)

# Удаляем старые хендлеры
if logger.hasHandlers():
    logger.handlers.clear()

handler = RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# Отключаем дублирование в stderr
logger.propagate = False

logger.info("🚀 Логгер инициализирован и файл очищен.")

# Удаление старой базы данных
try:
    if os.path.exists("asic_proxy.db"):
        os.remove("asic_proxy.db")
        logger.info("[CLEANUP] Удалён старый файл базы данных: asic_proxy.db")
except Exception as e:
    logger.error(f"[CLEANUP ERROR] Ошибка при удалении базы: {e}")

# Конфигурация пула
POOL_HOST = "solo.ckpool.org"
POOL_PORT = 3333
# Конфигурация прокси для ASIC
ASIC_PROXY_HOST = "0.0.0.0"
ASIC_PROXY_PORT = 3333

# Путь к базе данных
DB_PATH = "asic_proxy.db"

# Версия приложения
VERSION = "1.0.0"

# Конфигурация Telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "7207281851:AAEzDaJmpvA6KB9xgTo7dnEbnW4LUtnH4FQ")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "480223056")
ENABLE_TELEGRAM = False

# Глобальные переменные для fix_hex_string
last_hex_warning_time = 0
hex_warning_shown = False

# Глобальные переменные для управления завершением
shutdown_event = asyncio.Event()

# Обработка сигналов для graceful shutdown
def handle_shutdown(signum, frame):
    logger.info("Получен сигнал завершения (Ctrl+C или SIGTERM), останавливаем сервер...")
    shutdown_event.set()
    if 'server' in globals():
        server.shutdown()
    if 'proxy' in globals():
        proxy.stop()

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
        if conn.laddr.port == port and conn.pid and conn.pid not in processed_pids:
            try:
                proc = psutil.Process(conn.pid)
                logger.info(f"[PORT IN USE] Убиваем процесс PID={conn.pid}, использующий порт :{port}")
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except psutil.TimeoutExpired:
                    logger.warning(f"[PORT] Процесс PID={conn.pid} не завершился, принудительно убиваем...")
                    proc.kill()
                processed_pids.add(conn.pid)
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Ошибка при завершении процесса PID={conn.pid}: {e}")
    timeout = 30
    start_time = time.time()
    while time.time() - start_time < timeout:
        port_in_use = any(p.laddr.port == port for p in psutil.net_connections(kind='inet'))
        if not port_in_use:
            logger.info(f"[PORT] Порт {port} освободился.")
            return
        await asyncio.sleep(0.5)
    logger.error(f"[PORT] Не удалось освободить порт {port} за {timeout} секунд")
    raise RuntimeError(f"Не удалось освободить порт {port}")

class TelegramHandler(logging.Handler):
    """Асинхронный обработчик логов для отправки в Telegram с поддержкой команд."""
    def __init__(self, token, chat_id, loop=None):
        super().__init__()
        self.token = token
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.enabled = ENABLE_TELEGRAM
        self.shutdown_flag = False
        self.loop = loop or asyncio.get_event_loop()

    async def send_message(self, message):
        """Асинхронная отправка сообщения в Telegram."""
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
                logger.error(f"[TelegramHandler] Ошибка при отправке в Telegram: {e}")

    def emit(self, record):
        if self.shutdown_flag or not self.enabled:
            return
        try:
            log_entry = self.format(record)
            keywords = ["submit", "notify", "asic", "pool", "share", "error"]
            if any(keyword in log_entry.lower() for keyword in keywords):
                message = f"🚨 <b>{record.levelname}</b>\n<code>{log_entry}</code>"
                if self.loop.is_closed() or not self.loop.is_running():
                    logger.debug("[TelegramHandler] Пропуск отправки — event loop неактивен")
                    return
                asyncio.run_coroutine_threadsafe(self.send_message(message), self.loop)
            if "/shutdown" in log_entry.lower() and not self.loop.is_closed():
                asyncio.run_coroutine_threadsafe(self.handle_shutdown(), self.loop)
        except Exception as e:
            logger.error(f"[TelegramHandler] Ошибка в emit: {e}")

    async def handle_shutdown(self):
        if self.loop.is_closed():
            logger.debug("[TelegramHandler] loop is closed — handle_shutdown пропущен")
            return
        await self.send_message("🛑 Выполняется завершение работы по команде /shutdown...")
        logger.info("[TELEGRAM] Получена команда /shutdown, завершаем работу...")
        self.disable()
        shutdown_event.set()
        if 'proxy' in globals():
            proxy.stop()
        if 'server' in globals():
            server.shutdown()

    def disable(self):
        self.shutdown_flag = True
        self.enabled = False

    def enable(self):
        self.enabled = ENABLE_TELEGRAM
        self.shutdown_flag = False

class ThreadedTCPServer(ThreadingMixIn, TCPServer):
    allow_reuse_address = True

class PoolConnector(Thread):
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
        self.db_lock = Lock()
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.cur = self.conn.cursor()
        self._init_db()
        self.pool_queue = asyncio.Queue()
        self.asic_queue = asyncio.Queue()
        self.running = True
        self.loop = asyncio.new_event_loop()
        self.request_id = 0
        self.pending_requests = {}
        self.last_received = time.time()

    def _get_unique_id(self):
        self.request_id += 1
        return self.request_id

    @contextmanager
    def get_db(self):
        with self.db_lock:
            try:
                yield self.conn
                self.conn.commit()
            except Exception as e:
                logger.error(f"[DB] Ошибка базы данных: {type(e).__name__}: {e}")
                self.conn.rollback()
                raise

    def _init_db(self):
        with self.get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS submits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    extranonce2 TEXT,
                    timestamp INTEGER,
                    accepted INTEGER
                )
            """)

    def run(self):
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._run_async())
        except Exception as e:
            logger.error(f"[POOL] Ошибка в run: {type(e).__name__}: {e}")
        finally:
            self._cleanup_loop()

    def _cleanup_loop(self):
        if not self.loop.is_closed():
            for task in asyncio.all_tasks(self.loop):
                task.cancel()
            try:
                self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            except Exception as e:
                logger.error(f"[POOL] Ошибка при очистке loop: {type(e).__name__}: {e}")
            self.loop.close()

    async def _read_pool_messages(self):
        buffer = ""
        self.last_received = time.time()
        while self.running and self.pool_socket and not shutdown_event.is_set():
            try:
                chunk = self.pool_socket.recv(4096).decode('utf-8')
                if not chunk:
                    logger.warning("[POOL] Соединение с пулом разорвано")
                    break
                logger.debug(f"[POOL RAW] Получено: {chunk}")
                self.last_received = time.time()
                buffer += chunk
                while '\n' in buffer:
                    message, buffer = buffer.split('\n', 1)
                    if not message.strip():
                        continue
                    try:
                        json_msg = json.loads(message)
                        logger.info(f"[POOL MSG] Обработано: {json_msg}")
                        self._handle_pool_message(json_msg)
                    except json.JSONDecodeError:
                        logger.error(f"[POOL] Некорректный JSON: {message}")
                        buffer = message + buffer
                        break
            except socket.timeout:
                if time.time() - self.last_received > 60:
                    logger.warning("[RECV] Нет данных от пула более 60 секунд, разрыв соединения")
                    break
                continue
            except ConnectionResetError:
                logger.warning("[RECV] Соединение сброшено пулом")
                break
            except Exception as e:
                logger.error(f"[RECV] Ошибка получения данных: {type(e).__name__}: {e}")
                break
        self.pool_socket = None

    async def subscribe_and_authorize(self):
        try:
            subscribe_msg = {
                "id": self._get_unique_id(),
                "method": "mining.subscribe",
                "params": ["cgminer/4.10.0"]
            }
            future = await self._send_to_pool(subscribe_msg)
            await asyncio.wait_for(future, timeout=10)
        except Exception as e:
            logger.error(f"[SUBSCRIBE] Ошибка mining.subscribe: {type(e).__name__}: {e}")

    async def _run_async(self):
        fallback_hosts = [(POOL_HOST, POOL_PORT)]  # Удалён резервный пул
        backoff = 5
        while self.running and not shutdown_event.is_set():
            for host, port in fallback_hosts:
                try:
                    logger.info(f"[POOL] Попытка подключения к пулу {host}:{port}...")
                    with socket.create_connection((host, port), timeout=10) as s:
                        self.pool_socket = s
                        self.pool_socket.settimeout(10.0)
                        logger.info(f"[POOL] Подключение к {host}:{port} успешно")

                        await self.subscribe_and_authorize()

                        tasks = [
                            self._read_pool_messages(),
                            self._process_pool_queue(),
                            self._process_asic_queue(),
                            self._keep_alive()
                        ]
                        await asyncio.gather(*[asyncio.create_task(t) for t in tasks])
                    backoff = 5
                    break
                except socket.timeout:
                    logger.warning(f"[POOL] Таймаут подключения к {host}:{port}, повторная попытка...")
                except socket.gaierror:
                    logger.error(f"[POOL] Ошибка DNS для {host}:{port}")
                except ConnectionRefusedError:
                    logger.error(f"[POOL] Пул {host}:{port} отклонил соединение")
                except Exception as e:
                    logger.error(f"[POOL] Ошибка в соединении с пулом {host}:{port}: {type(e).__name__}: {e}")
                finally:
                    if self.pool_socket:
                        try:
                            self.pool_socket.close()
                        except:
                            pass
                        self.pool_socket = None
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
            if shutdown_event.is_set():
                break

class ASICHandler(StreamRequestHandler):
    """Обработчик подключений от ASIC-устройств."""
    def handle(self):
        logger.info(f"[ASIC] Подключение от {self.client_address[0]}")
        self.server.asic_socket = self.request
        while True:
            try:
                data = self.request.recv(4096).decode('utf-8')
                if not data:
                    logger.warning("[ASIC] Соединение с ASIC разорвано")
                    break
                logger.debug(f"[ASIC DATA] Получено от ASIC: {data}")
                try:
                    msg = json.loads(data)
                    if not isinstance(msg, dict) or "id" not in msg:
                        logger.error(f"[ASIC] Некорректный формат сообщения: {data}")
                        continue
                    if msg.get("method") == "mining.configure":
                        response = {
                            "id": msg["id"],
                            "result": {"version-rolling": True},
                            "error": None
                        }
                        self.request.sendall((json.dumps(response) + '\n').encode('utf-8'))
                        logger.info(f"[ASIC] Отправлен ответ на mining.configure: {response}")
                        asyncio.run_coroutine_threadsafe(
                            self.server.proxy._send_to_pool({
                                "method": "mining.configure",
                                "params": ["version-rolling"]
                            }),
                            self.server.proxy.loop
                        )
                    elif msg.get("method") == "mining.subscribe":
                        logger.info("[ASIC] Получен mining.subscribe")
                        try:
                            future = asyncio.run_coroutine_threadsafe(
                                self.server.proxy._send_to_pool({
                                    "method": "mining.subscribe",
                                    "params": ["ASICProxy/1.0.0"]
                                }),
                                self.server.proxy.loop
                            )
                            result = future.result(timeout=30)
                            if not self.server.proxy.extranonce1 or not self.server.proxy.extranonce2_size:
                                logger.error("[ASIC] Не получен extranonce1 от пула")
                                response = {
                                    "id": msg["id"],
                                    "result": None,
                                    "error": "Failed to receive extranonce from pool"
                                }
                                self.request.sendall((json.dumps(response) + '\n').encode('utf-8'))
                                return
                            response = {
                                "id": msg["id"],
                                "result": [["mining.notify", "ae6812eb4cd7735a"], self.server.proxy.extranonce1, self.server.proxy.extranonce2_size],
                                "error": None
                            }
                            self.request.sendall((json.dumps(response) + '\n').encode('utf-8'))
                            logger.info(f"[ASIC] Отправлен ответ на mining.subscribe: {response}")
                            asyncio.run_coroutine_threadsafe(
                                self.server.proxy._send_to_pool({
                                    "method": "mining.authorize",
                                    "params": ["YOUR_USERNAME.worker1", "YOUR_PASSWORD"]  # REPLACE WITH VALID CREDENTIALS
                                }),
                                self.server.proxy.loop
                            )
                        except TimeoutError:
                            logger.error("[ASIC] Таймаут ожидания ответа на mining.subscribe")
                            response = {
                                "id": msg["id"],
                                "result": None,
                                "error": "Timeout waiting for pool response"
                            }
                            self.request.sendall((json.dumps(response) + '\n').encode('utf-8'))
                            return
                        except Exception as e:
                            logger.error(f"[ASIC] Ошибка при обработке mining.subscribe: {type(e).__name__}: {e}")
                            response = {
                                "id": msg["id"],
                                "result": None,
                                "error": f"Failed to process subscribe: {e}"
                            }
                            self.request.sendall((json.dumps(response) + '\n').encode('utf-8'))
                            return
                    elif msg.get("method") == "mining.authorize":
                        if hasattr(self.server, 'proxy'):
                            asyncio.run_coroutine_threadsafe(self.server.proxy._send_to_pool(msg), self.server.proxy.loop)
                            response = {"id": msg["id"], "result": True, "error": None}
                            self.request.sendall((json.dumps(response) + '\n').encode('utf-8'))
                            logger.info(f"[ASIC] Отправлен ответ на mining.authorize: {response}")
                            self.server.proxy.authorized = True
                    elif msg.get("method") == "mining.submit" and hasattr(self.server, 'proxy'):
                        asyncio.run_coroutine_threadsafe(self.server.proxy._send_to_pool(msg), self.server.proxy.loop)
                        logger.info(f"[ASIC] Шара получена и добавлена в очередь для пула: {msg}")
                    elif hasattr(self.server, 'proxy'):
                        asyncio.run_coroutine_threadsafe(self.server.proxy._send_to_pool(msg), self.server.proxy.loop)
                except json.JSONDecodeError:
                    logger.error(f"[ASIC DATA] Некорректный JSON от ASIC: {data}")
            except Exception as e:
                logger.error(f"[ASIC] Ошибка чтения данных от ASIC: {type(e).__name__}: {e}")
                break
        self.server.asic_socket = None

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
        self.db_lock = Lock()
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.cur = self.conn.cursor()
        self._init_db()
        self.pool_queue = asyncio.Queue()
        self.asic_queue = asyncio.Queue()
        self.running = True
        self.loop = asyncio.new_event_loop()
        self.request_id = 0
        self.pending_requests = {}
        self.last_received = time.time()

    def _get_unique_id(self):
        self.request_id += 1
        return self.request_id

    @contextmanager
    def get_db(self):
        with self.db_lock:
            try:
                yield self.conn
                self.conn.commit()
            except Exception as e:
                logger.error(f"[DB] Ошибка базы данных: {type(e).__name__}: {e}")
                self.conn.rollback()
                raise

    def _init_db(self):
        with self.get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS submits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    extranonce2 TEXT,
                    timestamp INTEGER,
                    accepted INTEGER
                )
            """)

    def run(self):
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._run_async())
        except Exception as e:
            logger.error(f"[POOL] Ошибка в run: {type(e).__name__}: {e}")
        finally:
            self._cleanup_loop()

    def _cleanup_loop(self):
        if not self.loop.is_closed():
            for task in asyncio.all_tasks(self.loop):
                task.cancel()
            try:
                self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            except Exception as e:
                logger.error(f"[POOL] Ошибка при очистке loop: {type(e).__name__}: {e}")
            self.loop.close()

    async def _read_pool_messages(self):
        buffer = ""
        self.last_received = time.time()
        while self.running and self.pool_socket and not shutdown_event.is_set():
            try:
                chunk = self.pool_socket.recv(4096).decode('utf-8')
                if not chunk:
                    logger.warning("[POOL] Соединение с пулом разорвано")
                    break
                if os.getenv("DEBUG_RAW", "false").lower() == "true":
                    logger.debug(f"[POOL RAW] Получено: {chunk}")
                self.last_received = time.time()
                buffer += chunk
                while '\n' in buffer:
                    message, buffer = buffer.split('\n', 1)
                    if not message.strip():
                        continue
                    try:
                        json_msg = json.loads(message)
                        logger.info(f"[POOL MSG] Обработано: {json_msg}")
                        self._handle_pool_message(json_msg)
                    except json.JSONDecodeError:
                        logger.error(f"[POOL] Некорректный JSON: {message}")
                        buffer = message + buffer
                        break
            except socket.timeout:
                if time.time() - self.last_received > 60:
                    logger.warning("[RECV] Нет данных от пула более 60 секунд, разрыв соединения")
                    break
                continue
            except ConnectionResetError:
                logger.warning("[RECV] Соединение сброшено пулом")
                break
            except Exception as e:
                logger.error(f"[RECV] Ошибка получения данных: {type(e).__name__}: {e}")
                break
        self.pool_socket = None

    async def subscribe_and_authorize(self):
        try:
            configure_msg = {
                "id": self._get_unique_id(),
                "method": "mining.configure",
                "params": [["version-rolling"], {"version-rolling.mask": "ffffffff"}]
            }
            future = await self._send_to_pool(configure_msg)
            await asyncio.wait_for(future, timeout=int(os.getenv("TIMEOUT_CONFIGURE", 10)))
        except Exception as e:
            logger.warning(f"[SUBSCRIBE] Ошибка mining.configure: {type(e).__name__}: {e}")

        try:
            subscribe_msg = {
                "id": self._get_unique_id(),
                "method": "mining.subscribe",
                "params": ["PythonProxy/1.0"]
            }
            future = await self._send_to_pool(subscribe_msg)
            await asyncio.wait_for(future, timeout=int(os.getenv("TIMEOUT_SUBSCRIBE", 10)))
        except Exception as e:
            logger.error(f"[SUBSCRIBE] Ошибка mining.subscribe: {type(e).__name__}: {e}")

        try:
            authorize_msg = {
                "id": self._get_unique_id(),
                "method": "mining.authorize",
                "params": ["AssetDurmagambet.worker1", "x"]
            }
            future = await self._send_to_pool(authorize_msg)
            await asyncio.wait_for(future, timeout=int(os.getenv("TIMEOUT_AUTHORIZE", 10)))
        except Exception as e:
            logger.error(f"[SUBSCRIBE] Ошибка mining.authorize: {type(e).__name__}: {e}")

    async def _run_async(self):
        fallback_hosts = [(POOL_HOST, POOL_PORT)]
        backoff = 5
        while self.running and not shutdown_event.is_set():
            for host, port in fallback_hosts:
                try:
                    logger.info(f"[POOL] Попытка подключения к пулу {host}:{port}...")
                    with socket.create_connection((host, port), timeout=10) as s:
                        self.pool_socket = s
                        self.pool_socket.settimeout(10.0)
                        logger.info(f"[POOL] Подключение к {host}:{port} успешно")

                        await self.subscribe_and_authorize()

                        tasks = [
                            self._read_pool_messages(),
                            self._process_pool_queue(),
                            self._process_asic_queue(),
                            self._keep_alive()
                        ]
                        await asyncio.gather(*[asyncio.create_task(t) for t in tasks])
                    backoff = 5
                    break
                except socket.timeout:
                    logger.warning(f"[POOL] Таймаут подключения к {host}:{port}, повторная попытка...")
                except socket.gaierror:
                    logger.error(f"[POOL] Ошибка DNS для {host}:{port}")
                except ConnectionRefusedError:
                    logger.error(f"[POOL] Пул {host}:{port} отклонил соединение")
                except Exception as e:
                    logger.error(f"[POOL] Ошибка в соединении с пулом {host}:{port}: {type(e).__name__}: {e}")
                finally:
                    if self.pool_socket:
                        try:
                            self.pool_socket.close()
                        except:
                            pass
                        self.pool_socket = None
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
            if shutdown_event.is_set():
                break

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
        self.db_lock = Lock()
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.cur = self.conn.cursor()
        self._init_db()
        self.pool_queue = asyncio.Queue()
        self.asic_queue = asyncio.Queue()
        self.running = True
        self.loop = asyncio.new_event_loop()
        self.request_id = 0
        self.pending_requests = {}
        self.last_received = time.time()

    def _get_unique_id(self):
        self.request_id += 1
        return self.request_id

    @contextmanager
    def get_db(self):
        with self.db_lock:
            try:
                yield self.conn
                self.conn.commit()
            except Exception as e:
                logger.error(f"[DB] Ошибка базы данных: {type(e).__name__}: {e}")
                self.conn.rollback()
                raise

    def _init_db(self):
        with self.get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS submits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    extranonce2 TEXT,
                    timestamp INTEGER,
                    accepted INTEGER
                )
            """)

    def run(self):
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._run_async())
        except Exception as e:
            logger.error(f"[POOL] Ошибка в run: {type(e).__name__}: {e}")
        finally:
            self._cleanup_loop()

    def _cleanup_loop(self):
        if not self.loop.is_closed():
            for task in asyncio.all_tasks(self.loop):
                task.cancel()
            try:
                self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            except Exception as e:
                logger.error(f"[POOL] Ошибка при очистке loop: {type(e).__name__}: {e}")
            self.loop.close()

    async def _read_pool_messages(self):
        buffer = ""
        self.last_received = time.time()
        while self.running and self.pool_socket and not shutdown_event.is_set():
            try:
                chunk = self.pool_socket.recv(4096).decode('utf-8')
                if not chunk:
                    logger.warning("[POOL] Соединение с пулом разорвано")
                    break
                if True:
                    logger.debug(f"[POOL RAW] Получено: {chunk}")
                self.last_received = time.time()
                buffer += chunk
                while '\n' in buffer:
                    message, buffer = buffer.split('\n', 1)
                    if not message.strip():
                        continue
                    try:
                        json_msg = json.loads(message)
                        logger.info(f"[POOL MSG] Обработано: {json_msg}")
                        self._handle_pool_message(json_msg)
                    except json.JSONDecodeError:
                        logger.error(f"[POOL] Некорректный JSON: {message}")
                        buffer = message + buffer
                        break
            except socket.timeout:
                if time.time() - self.last_received > 60:
                    logger.warning("[RECV] Нет данных от пула более 60 секунд, разрыв соединения")
                    break
                continue
            except ConnectionResetError:
                logger.warning("[RECV] Соединение сброшено пулом")
                break
            except Exception as e:
                logger.error(f"[RECV] Ошибка получения данных: {type(e).__name__}: {e}")
                break
        self.pool_socket = None

    async def subscribe_and_authorize(self):
        try:
            configure_msg = {
                "id": self._get_unique_id(),
                "method": "mining.configure",
                "params": [["version-rolling"], {"version-rolling.mask": "ffffffff"}]
            }
            future = await self._send_to_pool(configure_msg)
            await asyncio.wait_for(future, timeout=int(os.getenv("TIMEOUT_CONFIGURE", 10)))
        except Exception as e:
            logger.warning(f"[SUBSCRIBE] Ошибка mining.configure: {type(e).__name__}: {e}")

        try:
            subscribe_msg = {
                "id": self._get_unique_id(),
                "method": "mining.subscribe",
                "params": ["PythonProxy/1.0"]
            }
            future = await self._send_to_pool(subscribe_msg)
            await asyncio.wait_for(future, timeout=int(os.getenv("TIMEOUT_SUBSCRIBE", 10)))
        except Exception as e:
            logger.error(f"[SUBSCRIBE] Ошибка mining.subscribe: {type(e).__name__}: {e}")

        try:
            authorize_msg = {
                "id": self._get_unique_id(),
                "method": "mining.authorize",
                "params": ["AssetDurmagambet.worker1", "x"]
            }
            future = await self._send_to_pool(authorize_msg)
            await asyncio.wait_for(future, timeout=int(os.getenv("TIMEOUT_AUTHORIZE", 10)))
        except Exception as e:
            logger.error(f"[SUBSCRIBE] Ошибка mining.authorize: {type(e).__name__}: {e}")

    async def _run_async(self):
        fallback_hosts = [(POOL_HOST, POOL_PORT)]
        backoff = 5
        while self.running and not shutdown_event.is_set():
            for host, port in fallback_hosts:
                try:
                    logger.info(f"[POOL] Попытка подключения к пулу {host}:{port}...")
                    with socket.create_connection((host, port), timeout=10) as s:
                        self.pool_socket = s
                        self.pool_socket.settimeout(10.0)
                        logger.info(f"[POOL] Подключение к {host}:{port} успешно")

                        await self.subscribe_and_authorize()

                        tasks = [
                            self._read_pool_messages(),
                            self._process_pool_queue(),
                            self._process_asic_queue(),
                            self._keep_alive()
                        ]
                        await asyncio.gather(*[asyncio.create_task(t) for t in tasks])
                    backoff = 5
                    break
                except socket.timeout:
                    logger.warning(f"[POOL] Таймаут подключения к {host}:{port}, повторная попытка...")
                except socket.gaierror:
                    logger.error(f"[POOL] Ошибка DNS для {host}:{port}")
                except ConnectionRefusedError:
                    logger.error(f"[POOL] Пул {host}:{port} отклонил соединение")
                except Exception as e:
                    logger.error(f"[POOL] Ошибка в соединении с пулом {host}:{port}: {type(e).__name__}: {e}")
                finally:
                    if self.pool_socket:
                        try:
                            self.pool_socket.close()
                        except:
                            pass
                        self.pool_socket = None
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
            if shutdown_event.is_set():
                break

def fix_hex_string(s):
    global last_hex_warning_time, hex_warning_shown
    current_time = time.time()
    if not s:
        s = "00"
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
    try:
        coinbase_bin = binascii.unhexlify(coinbase)
        coinbase_hash = hashlib.sha256(hashlib.sha256(coinbase_bin).digest()).digest()
        merkle_root = coinbase_hash
        for branch in branches:
            branch_bin = binascii.unhexlify(branch)
            merkle_root = hashlib.sha256(hashlib.sha256(merkle_root + branch_bin).digest()).digest()
        return merkle_root.hex()
    except binascii.Error as e:
        logger.error(f"[MERKLE ROOT] Ошибка преобразования hex: {e}")
        return ""

async def main_async():
    global server, proxy, telegram_handler
    logger.info("[STARTUP] Программа начала выполнение...")
    local_server = None
    local_proxy = None
    local_telegram_handler = None
    try:
        logger.info(f"[PORT] Проверка порта {ASIC_PROXY_PORT}...")
        await ensure_port_available(ASIC_PROXY_PORT)
        logger.info(f"[PORT] Порт {ASIC_PROXY_PORT} доступен.")
        logger.info(f"[SERVER] Запуск TCP-сервера на {ASIC_PROXY_HOST}:{ASIC_PROXY_PORT}...")
        local_server = ThreadedTCPServer((ASIC_PROXY_HOST, ASIC_PROXY_PORT), ASICHandler)
        local_proxy = PoolConnector(local_server)
        local_server.proxy = local_proxy
        server = local_server
        proxy = local_proxy
        local_proxy.start()
        threading.Thread(target=local_server.serve_forever, daemon=True).start()
        logger.info(f"[SERVER] Прокси-сервер запущен на {ASIC_PROXY_HOST}:{ASIC_PROXY_PORT}")

        async def handle_status(request):
            return web.json_response({
                "version": VERSION,
                "accepted_shares": proxy.accepted_shares,
                "rejected_shares": proxy.rejected_shares,
                "connected": proxy.pool_socket is not None,
                "pool_queue_size": proxy.pool_queue.qsize(),
                "asic_queue_size": proxy.asic_queue.qsize(),
                "authorized": proxy.authorized
            })

        async def handle_shutdown(request):
            if local_telegram_handler:
                await local_telegram_handler.handle_shutdown()
            return web.Response(text="Shutting down...")

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

        current_loop = asyncio.get_running_loop()
        local_telegram_handler = TelegramHandler(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, loop=current_loop)
        local_telegram_handler.setLevel(logging.DEBUG)
        local_telegram_handler.setFormatter(formatter)
        if ENABLE_TELEGRAM:
            logger.addHandler(local_telegram_handler)
        telegram_handler = local_telegram_handler

        while not shutdown_event.is_set():
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"[MAIN] Ошибка в main_async: {type(e).__name__}: {e}")
        raise
    finally:
        logger.info("[SHUTDOWN] Закрываем ресурсы...")
        if local_proxy:
            local_proxy.stop()
        if local_server:
            local_server.shutdown()
            local_server.server_close()
        if local_telegram_handler:
            local_telegram_handler.disable()
        await asyncio.sleep(1)

if __name__ == "__main__":
    logger.info("🔥 Запуск основного блока if __name__ == '__main__'")
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(main_async())
    except KeyboardInterrupt:
        logger.warning("🛑 Остановка по Ctrl+C")
        shutdown_event.set()
        if 'telegram_handler' in globals():
            telegram_handler.disable()
        if 'proxy' in globals():
            proxy.stop()
        if 'server' in globals():
            server.shutdown()
            server.server_close()
        logger.info("[SERVER] Сервер корректно остановлен.")
    except Exception as e:
        logger.error(f"[MAIN] Необработанная ошибка: {type(e).__name__}: {e}")
    finally:
        if 'loop' in locals() and not loop.is_closed():
            for task in asyncio.all_tasks(loop):
                task.cancel()
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
        logger.info("[MAIN] Event loop закрыт")
        sys.exit(0)