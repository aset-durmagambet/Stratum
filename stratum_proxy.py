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

print("✅ Текущая директория:", os.getcwd())
print("📄 Содержимое:", os.listdir())

LOG_FILE = "/root/Stratum/asic_proxy.log"

# Создаём лог-файл, если папки нет
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

try:
    if os.path.exists("asic_proxy.db"):
        os.remove("asic_proxy.db")
        logger.info("[CLEANUP] Удалён старый файл базы данных: asic_proxy.db")
except Exception as e:
    logger.error(f"[CLEANUP ERROR] Ошибка при удалении базы: {e}")

# Конфигурация пула — явно
POOL_HOST = "stratum.braiins.com"
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
# Включить/отключить Telegram-уведомления
ENABLE_TELEGRAM = False
# Инициализация логгера


from logging.handlers import RotatingFileHandler

# Путь к лог-файлу
log_file_path = "asic_proxy.log"

# Очистка лог-файла при старте
with open(log_file_path, "w") as f:
    f.write("")

# Инициализация логгера
logger = logging.getLogger("ASICProxy")
logger.setLevel(logging.DEBUG)  # или INFO, если логов слишком много

# Удаление старых хендлеров
if logger.hasHandlers():
    logger.handlers.clear()

# Создание нового хендлера
handler = RotatingFileHandler(log_file_path, maxBytes=10*1024*1024, backupCount=5)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.info("Логгер инициализирован. Файл очищен.")

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
    def __init__(self, token, chat_id, loop):
        super().__init__()
        self.token = token
        self.chat_id = chat_id
        self.loop = loop
        self.enabled = False  # 🔴 По умолчанию отключено

    def emit(self, record):
        if not self.enabled:
            return  # ⚠ Неактивно — не отправляем

        try:
            log_entry = self.format(record)

            # Проверка: есть ли активный event loop
            if not self.loop or self.loop.is_closed():
                print("[TelegramHandler] Пропуск отправки — event loop неактивен")
                return

            # Асинхронная отправка
            asyncio.run_coroutine_threadsafe(self.send_telegram(log_entry), self.loop)
        except Exception as e:
            print(f"[TelegramHandler] Ошибка в emit: {e}")  # избегаем логгера!

    async def send_telegram(self, message):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=payload) as resp:
                    if resp.status != 200:
                        print(f"[TelegramHandler] Telegram error: {await resp.text()}")
        except Exception as e:
            print(f"[TelegramHandler] Telegram send failed: {e}")

    def enable(self):
        self.enabled = True

    def disable(self):
        self.enabled = False

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
                                # Run the coroutine in the event loop and get the result
                                future = asyncio.run_coroutine_threadsafe(
                                    self.server.proxy._send_to_pool({
                                        "method": "mining.subscribe",
                                        "params": ["ASICProxy/1.0.0"]
                                    }),
                                    self.server.proxy.loop
                                )
                                # Wait for the result synchronously with a timeout
                                result = future.result(timeout=5)
                                
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
                                    "result": [["mining.notify", "ae6812eb4cd7735a"]],
                                    "extranonce1": self.server.proxy.extranonce1,
                                    "extranonce2_size": self.server.proxy.extranonce2_size
                                }
                                self.request.sendall((json.dumps(response) + '\n').encode('utf-8'))
                                logger.info(f"[ASIC] Отправлен ответ на mining.subscribe: {response}")
                                asyncio.run_coroutine_threadsafe(
                                    self.server.proxy._send_to_pool({
                                        "method": "mining.authorize",
                                        "params": ["AssetDurmagambet.worker1", "x"]
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
                                logger.error(f"[ASIC] Ошибка при обработке mining.subscribe: {e}")
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
                else:
                    logger.warning("[ASIC] Соединение с ASIC разорвано")
                    break
            except Exception as e:
                logger.error(f"[ASIC] Ошибка чтения данных от ASIC: {e}")
                break

class PoolConnector(Thread):
    """Соединитель с майнинг-пулом с поддержкой очередей для пула и ASIC."""
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

    def _get_unique_id(self):
        """Генерирует уникальный ID для запросов к пулу."""
        self.request_id += 1
        return self.request_id

    @contextmanager
    def get_db(self):
        """Контекстный менеджер для безопасной работы с SQLite."""
        with self.db_lock:
            try:
                yield self.conn
                self.conn.commit()
            except Exception as e:
                logger.error(f"[DB] Ошибка базы данных: {e}")
                self.conn.rollback()
                raise

    def _init_db(self):
        """Инициализация базы данных."""
        with self.get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS submits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    extranonce2 TEXT,
                    timestamp INTEGER
                )
            """)

    def run(self):
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._run_async())
        except Exception as e:
            logger.error(f"[POOL] Ошибка в run: {e}")
        finally:
            self._cleanup_loop()

    def _cleanup_loop(self):
        """Очистка асинхронного цикла."""
        if not self.loop.is_closed():
            for task in asyncio.all_tasks(self.loop):
                task.cancel()
            try:
                self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            except Exception as e:
                logger.error(f"[POOL] Ошибка при очистке loop: {e}")
            self.loop.close()

    async def wait_for_extranonce(self, timeout=5):
        """Асинхронное ожидание получения extranonce от пула."""
        try:
            await asyncio.sleep(0)
            for _ in range(int(timeout / 0.1)):
                if self.extranonce1 and self.extranonce2_size:
                    return True
                await asyncio.sleep(0.1)
            logger.error("[POOL] Таймаут ожидания extranonce")
            return False
        except Exception as e:
            logger.error(f"[POOL] Ошибка при ожидании extranonce: {e}")
            return False

    async def _run_async(self):
        fallback_hosts = [(POOL_HOST, POOL_PORT), ("stratum.braiins.com", POOL_PORT)]
        while self.running and not shutdown_event.is_set():
            for host, port in fallback_hosts:
                try:
                    logger.info(f"[POOL] Попытка подключения к пулу {host}:{port}...")
                    with socket.create_connection((host, port), timeout=5) as s:
                        self.pool_socket = s
                        self.pool_socket.settimeout(5.0)
                        logger.info(f"[POOL] Подключение к {host}:{port} успешно")
                        await self._subscribe()
                        await asyncio.gather(
                            self._read_pool_messages(),
                            self._process_pool_queue(),
                            self._process_asic_queue(),
                            self._keep_alive()
                        )
                    break
                except Exception as e:
                    logger.error(f"[POOL] Ошибка в соединении с пулом {host}:{port}: {e}")
                    self.pool_socket = None
                    await asyncio.sleep(5)
                finally:
                    if self.pool_socket:
                        self.pool_socket.close()
                        self.pool_socket = None
            if shutdown_event.is_set():
                break

    async def _keep_alive(self):
        """Отправка keep-alive сообщений для поддержания соединения."""
        while self.running and self.pool_socket:
            try:
                await self._send_to_pool({"method": "mining.ping", "params": []})
                await asyncio.sleep(10 if not self.authorized else 30)
            except Exception as e:
                logger.error(f"[KEEP ALIVE] Ошибка: {e}")
                break

    async def _read_pool_messages(self):
        while self.running and self.pool_socket and not shutdown_event.is_set():
            try:
                buffer = ""
                while True:
                    chunk = self.pool_socket.recv(4096).decode('utf-8')
                    if not chunk:
                        break
                    buffer += chunk
                    if buffer.count('{') == buffer.count('}') and buffer.endswith('\n'):
                        break
                if buffer:
                    for message in self._split_messages(buffer):
                        try:
                            self._handle_pool_message(json.loads(message))
                        except Exception as e:
                            logger.error(f"Ошибка обработки сообщения от пула: {e}\n{message}")
                else:
                    logger.warning("[POOL] Соединение с пулом разорвано")
                    break
            except socket.timeout:
                logger.warning("[RECV] Таймаут получения данных от пула")
                continue
            except Exception as e:
                logger.error(f"[RECV] Ошибка получения данных: {e}")
                break
        self.pool_socket = None

    async def _process_pool_queue(self):
        while self.running and not shutdown_event.is_set():
            try:
                if self.pool_queue.qsize() > 100:
                    logger.warning("[POOL QUEUE] Очередь переполнена")
                msg = await self.pool_queue.get()
                if not self.pool_socket or self.pool_socket.fileno() == -1:
                    logger.warning("[POOL] Соединение потеряно, переподключаемся...")
                    self.pool_queue.put_nowait(msg)
                    break
                if msg.get("method") not in ["mining.subscribe", "mining.authorize", "mining.configure", "mining.ping"] and not self.authorized:
                    logger.warning("[POOL] Пропущена отправка, авторизация не завершена")
                    self.pool_queue.put_nowait(msg)
                    await asyncio.sleep(1)
                    continue
                try:
                    message = self._prepare_message(msg)
                    self.pool_socket.sendall(message.encode('utf-8'))
                    logger.debug(f"[POOL SEND] Отправлено в пул: {msg}")
                except Exception as e:
                    logger.error(f"[POOL SEND] Ошибка отправки: {e}")
                    self.pool_queue.put_nowait(msg)
                    self.pool_socket = None
                    break
                finally:
                    self.pool_queue.task_done()
            except Exception as e:
                logger.error(f"[POOL QUEUE] Ошибка обработки очереди: {e}")
                await asyncio.sleep(1)

    async def _process_asic_queue(self):
        while self.running and not shutdown_event.is_set():
            try:
                if self.asic_queue.qsize() > 100:
                    logger.warning("[ASIC QUEUE] Очередь переполнена")
                msg = await self.asic_queue.get()
                if not self.asic_socket or self.asic_socket.fileno() == -1:
                    logger.warning("[ASIC] ASIC-сокет недоступен")
                    self.asic_queue.put_nowait(msg)
                    self._reconnect_asic()
                    await asyncio.sleep(1)
                    continue
                try:
                    message = self._prepare_message(msg)
                    self.asic_socket.sendall(message.encode('utf-8'))
                    logger.debug(f"[ASIC SEND] Отправлено в ASIC: {msg}")
                except Exception as e:
                    logger.error(f"[ASIC SEND] Ошибка отправки: {e}")
                    self.asic_queue.put_nowait(msg)
                    self._reconnect_asic()
                finally:
                    self.asic_queue.task_done()
            except Exception as e:
                logger.error(f"[ASIC QUEUE] Ошибка обработки очереди: {e}")
                await asyncio.sleep(1)

    def _prepare_message(self, msg):
        """Проверяет и сериализует JSON-сообщение."""
        if not isinstance(msg, dict):
            raise ValueError(f"Сообщение должно быть словарем: {msg}")
        try:
            return json.dumps(msg, ensure_ascii=False) + '\n'
        except ValueError as e:
            logger.error(f"[JSON] Некорректный JSON: {msg}, ошибка: {e}")
            raise

    def _split_messages(self, data):
        return data.strip().split('\n')

    async def _send_to_pool(self, msg):
        """Отправляет сообщение в пул с уникальным ID и возвращает Future для ответа."""
        original_id = msg.get("id")
        msg["id"] = self._get_unique_id()
        future = asyncio.Future()
        self.pending_requests[msg["id"]] = {"original_id": original_id, "method": msg.get("method"), "future": future}
        await self.pool_queue.put(msg)
        logger.debug(f"[POOL QUEUE] Добавлено: {msg}, оригинальный ID: {original_id}")
        return future

    def _send_to_asic(self, msg):
        """Отправляет сообщение в ASIC."""
        asyncio.run_coroutine_threadsafe(self.asic_queue.put(msg), self.loop)
        logger.debug(f"[ASIC QUEUE] Добавлено: {msg}")

    def _reconnect_asic(self):
        logger.warning("[RECONNECT] Попытка переподключения к ASIC...")
        try:
            if self.asic_socket and self.asic_socket.fileno() != -1:
                self.asic_socket.close()
            self.asic_socket = None
            while not self.asic_socket and self.running and not shutdown_event.is_set():
                if hasattr(self.server, 'asic_socket') and self.server.asic_socket.fileno() != -1:
                    self.asic_socket = self.server.asic_socket
                    logger.info("[RECONNECT] Успешное переподключение к ASIC")
                    break
                time.sleep(1)
        except Exception as e:
            logger.error(f"[RECONNECT] Ошибка при переподключении к ASIC: {e}")

    async def _subscribe(self):
        """Отправляет команды настройки, подписки и авторизации."""
        await self._send_to_pool({"method": "mining.configure", "params": ["version-rolling"]})
        await self._send_to_pool({"method": "mining.subscribe", "params": ["ASICProxy/1.0.0"]})
        await self._send_to_pool({"method": "mining.authorize", "params": ["AssetDurmagambet.worker1", "x"]})

    def _handle_pool_message(self, msg):
        """Обрабатывает сообщения от пула."""
        logger.debug(f"Сообщение от пула: {msg}")
        msg_id = msg.get("id")
        request_info = self.pending_requests.pop(msg_id, None) if msg_id else None
        if request_info and "future" in request_info:
            future = request_info["future"]
        else:
            future = None
        if "error" in msg and msg["error"] is not None:
            logger.error(f"[POOL] Ошибка: {msg['error']}, запрос: {request_info}")
            if future and not future.done():
                future.set_exception(Exception(f"Pool error: {msg['error']}"))
            return
        if msg.get("method") == "mining.set_difficulty":
            self.difficulty = msg['params'][0]
            self._send_to_asic({
                "id": None,
                "method": "mining.set_difficulty",
                "params": [self.difficulty]
            })
        elif msg.get("method") == "mining.notify" and self.authorized:
            params = msg.get("params", [])
            if len(params) < 8:
                logger.error(f"Некорректный формат notify: {params}")
                return
            self._handle_notify(params)
        elif msg.get("result") and isinstance(msg['result'], list) and request_info and request_info.get("method") == "mining.subscribe":
            if len(msg['result']) < 3 or not isinstance(msg['result'][1], str) or not isinstance(msg['result'][2], int):
                logger.error(f"Некорректный ответ на subscribe: {msg}")
                if future and not future.done():
                    future.set_exception(Exception("Invalid subscribe response"))
                return
            self.extranonce1 = msg['result'][1]
            self.extranonce2_size = msg['result'][2]
            logger.info(f"[EXTRANONCE1] Получен: {self.extranonce1}, размер: {self.extranonce2_size}")
            if future and not future.done():
                future.set_result(True)
        elif msg_id and request_info and request_info.get("method") == "mining.authorize" and msg.get("result") is True:
            self.authorized = True
            logger.info("[POOL] Авторизация успешна")
            if future and not future.done():
                future.set_result(True)
        elif msg_id and "method" not in msg and not isinstance(msg.get("result"), list):
            if msg.get("result") is True:
                self.accepted_shares += 1
                logger.info("[SHARE] Принята")
                if future and not future.done():
                    future.set_result(True)
            else:
                self.rejected_shares += 1
                error = msg.get("error", "Неизвестная ошибка")
                logger.warning(f"[SHARE] Отклонена: код={error.get('code', 'N/A')}, причина={error.get('message', 'N/A')}")
                with self.get_db() as conn:
                    conn.execute(
                        "INSERT INTO submits (extranonce2, timestamp) VALUES (?, ?)",
                        ("rejected", int(time.time()))
                    )
                if future and not future.done():
                    future.set_exception(Exception(f"Share rejected: {error}"))

    def _handle_notify(self, params):
        try:
            job_id, coinb1, coinb2, merkle_branch, version, nbits, ntime, clean_jobs = params[:8]
            logger.debug(f"[RAW NOTIFY] job_id={job_id}, coinb1={coinb1}, coinb2={coinb2}")
            coinb1 = fix_hex_string(coinb1)
            coinb2 = fix_hex_string(coinb2)
            extranonce2 = self._select_best_extranonce2(job_id)
            extranonce1 = fix_hex_string(self.extranonce1) if self.extranonce1 else "00"
            merkle_root = calculate_merkle_root(coinb1, coinb2, extranonce1, extranonce2, merkle_branch)
            logger.info(f"[MERKLE ROOT] job_id={job_id} → {merkle_root}")
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
        best = top[-1][0] if top else "00000000"
        return best

    def stop(self):
        self.running = False
        if self.pool_socket:
            self.pool_socket.close()
        if self.asic_socket:
            self.asic_socket.close()
        self.conn.close()
        self._cleanup_loop()

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
                "asic_queue_size": proxy.asic_queue.qsize()
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
        logger.addHandler(local_telegram_handler)
        telegram_handler = local_telegram_handler

        while not shutdown_event.is_set():
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"[MAIN] Ошибка в main_async: {e}")
        raise
    finally:
        logger.info("Закрываем ресурсы...")
        if local_proxy:
            local_proxy.stop()
        if local_server:
            local_server.shutdown()
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
        logger.info("[SERVER] Сервер корректно остановлен.")
    except Exception as e:
        logger.error(f"[MAIN] Необработанная ошибка: {e}")
    finally:
        if not loop.is_closed():
            for task in asyncio.all_tasks(loop):
                task.cancel()
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
        logger.info("[MAIN] Event loop закрыт")
        sys.exit(0)