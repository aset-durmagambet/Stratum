import asyncio  # Импорт библиотеки для асинхронного программирования
import json  # Импорт библиотеки для работы с JSON (чтение/запись настроек)
import logging  # Импорт библиотеки для логирования сообщений
from typing import List, Dict  # Импорт типов для аннотаций (список и словарь)
from datetime import datetime, timedelta  # Импорт классов для работы с датой и временем
import hashlib  # Импорт библиотеки для вычисления хешей (SHA-256)
import binascii  # Импорт библиотеки для преобразования данных в/из шестнадцатеричного формата
import os  # Импорт библиотеки для работы с операционной системой (чтение файлов)

# Загрузка конфигурации из файла
CONFIG_FILE = "config.json"  # Имя файла конфигурации

def load_config(file_path: str) -> dict:  # Функция для загрузки конфигурации из файла
    try:  # Попытка выполнить блок кода
        with open(file_path, 'r') as f:  # Открытие файла config.json для чтения
            config = json.load(f)  # Чтение и преобразование JSON в словарь Python
        return config  # Возврат словаря с настройками
    except FileNotFoundError:  # Обработка ошибки, если файл не найден
        logging.error(f"Config file {file_path} not found. Using defaults.")  # Логирование ошибки
        return {}  # Возврат пустого словаря (будут использованы значения по умолчанию)
    except json.JSONDecodeError:  # Обработка ошибки, если JSON некорректен
        logging.error(f"Invalid JSON in {file_path}. Using defaults.")  # Логирование ошибки
        return {}  # Возврат пустого словаря

# Установка конфигурации
config = load_config(CONFIG_FILE)  # Загрузка настроек из файла config.json

# Константы с значениями по умолчанию из конфигурации
ASIC_IP = config.get("asic_ip", "192.168.1.10")  # IP-адрес ASIC, по умолчанию "192.168.1.10"
PROXY_IP = config.get("proxy_ip", "192.168.1.100")  # IP-адрес прокси, по умолчанию "192.168.1.100"
PROXY_PORT = config.get("proxy_port", 8888)  # Порт прокси, по умолчанию 8888
POOL_IP = config.get("pool_ip", "203.0.113.5")  # IP-адрес пула, по умолчанию "203.0.113.5"
POOL_PORT = config.get("pool_port", 3333)  # Порт пула, по умолчанию 3333
SHARE_LOG_INTERVAL = config.get("share_log_interval", 600)  # Интервал логирования шар (в секундах), по умолчанию 600
EXTRANONCE2_VARIANTS = config.get("extranonce2_variants", 10)  # Количество вариантов extranonce2, по умолчанию 10
WORKER_NAME = config.get("worker_name", "worker1")  # Имя воркера для пула, по умолчанию "worker1"
WORKER_PASSWORD = config.get("worker_password", "password")  # Пароль воркера, по умолчанию "password"

# Настройка логирования
log_level = getattr(logging, config.get("log_level", "INFO").upper(), logging.INFO)  # Уровень логирования из конфига (INFO по умолчанию)
logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')  # Настройка формата логов
logger = logging.getLogger("StratumProxy")  # Создание логгера с именем "StratumProxy"

class StratumProxy:  # Определение класса StratumProxy
    def __init__(self):  # Инициализация экземпляра класса
        self.pool_writer = None  # Переменная для записи данных в пул (будет установлена позже)
        self.pool_reader = None  # Переменная для чтения данных из пула (будет установлена позже)
        self.jobs: List[Dict] = []  # Список заданий для майнинга
        self.current_difficulty = 0  # Текущая сложность майнинга
        self.share_count = 0  # Счетчик отправленных шар
        self.last_job_time = None  # Время последнего отправленного задания
        self.asic_writer = None  # Переменная для записи данных в ASIC (будет установлена позже)
        self.extranonce1 = None  # Первая часть extranonce от пула
        self.extranonce2_size = 4  # Размер второй части extranonce (по умолчанию 4 байта)
        self.running = True  # Флаг для управления работой прокси (True = работает)

    def calculate_merkle_root(self, coinbase: str, merkle_branch: List[str], extranonce2: str) -> str:  # Вычисление корня Меркла
        coinbase_bytes = binascii.unhexlify(coinbase) + binascii.unhexlify(self.extranonce1) + binascii.unhexlify(extranonce2)  # Сборка coinbase из строк в байты
        coinbase_hash = hashlib.sha256(hashlib.sha256(coinbase_bytes).digest()).digest()  # Двойное хеширование coinbase
        merkle_root = coinbase_hash  # Начальное значение корня Меркла
        for branch in merkle_branch:  # Цикл по ветвям Меркла
            merkle_root = hashlib.sha256(hashlib.sha256(merkle_root + binascii.unhexlify(branch)).digest()).digest()  # Обновление корня с каждой ветвью
        return merkle_root.hex()  # Возврат корня в шестнадцатеричном формате

    def calculate_block_hash(self, version: str, prevhash: str, merkle_root: str, ntime: str, nbits: str) -> bytes:  # Вычисление хеша блока
        header = (  # Сборка заголовка блока из параметров
            binascii.unhexlify(version) +  # Версия протокола
            binascii.unhexlify(prevhash) +  # Хеш предыдущего блока
            binascii.unhexlify(merkle_root) +  # Корень Меркла
            binASIC.unhexlify(ntime) +  # Время
            binascii.unhexlify(nbits) +  # Сложность
            b'\x00\x00\x00\x00'  # Паддинг (nonce, будет добавлен ASIC)
        )
        return hashlib.sha256(hashlib.sha256(header).digest()).digest()  # Двойное хеширование заголовка

    def nbits_to_target(self, nbits: str) -> int:  # Преобразование nbits в целевое значение сложности
        exponent = int(nbits[:2], 16)  # Извлечение экспоненты из первых двух символов nbits
        coefficient = int(nbits[2:], 16)  # Извлечение коэффициента из оставшихся символов
        return coefficient * (2 ** (8 * (exponent - 3)))  # Вычисление целевого значения

    async def filter_jobs(self, job: Dict) -> List[Dict]:  # Фильтрация заданий для майнинга
        params = job["params"]  # Извлечение параметров задания
        job_id, prevhash, coinb1, coinb2, merkle_branch, version, nbits, ntime = params[:8]  # Разбор параметров
        coinbase = coinb1 + coinb2  # Сборка coinbase из двух частей
        target = self.nbits_to_target(nbits)  # Вычисление целевого значения сложности

        filtered_jobs = []  # Список отфильтрованных заданий
        for i in range(EXTRANONCE2_VARIANTS):  # Цикл по количеству вариантов extranonce2
            extranonce2 = (i * 2).to_bytes(self.extranonce2_size, byteorder='little').hex()  # Генерация extranonce2
            merkle_root = self.calculate_merkle_root(coinbase, merkle_branch, extranonce2)  # Вычисление корня Меркла
            block_hash = self.calculate_block_hash(version, prevhash, merkle_root, ntime, nbits)  # Вычисление хеша блока
            block_hash_int = int.from_bytes(block_hash, byteorder='little')  # Преобразование хеша в целое число

            if block_hash_int < target:  # Проверка, удовлетворяет ли хеш сложности
                filtered_job = job.copy()  # Копирование задания
                filtered_job["extranonce2"] = extranonce2  # Добавление extranonce2
                filtered_jobs.append(filtered_job)  # Добавление задания в список
                logger.debug(f"Job variant accepted: job_id={job_id}, extranonce2={extranonce2}")  # Логирование принятого варианта
        return filtered_jobs  # Возврат списка отфильтрованных заданий

    async def connect_to_pool(self):  # Подключение к пулу майнинга
        while self.running:  # Цикл работает, пока прокси активен
            try:  # Попытка подключения
                logger.info(f"Connecting to pool at {POOL_IP}:{POOL_PORT}")  # Логирование попытки подключения
                self.pool_reader, self.pool_writer = await asyncio.open_connection(POOL_IP, POOL_PORT)  # Открытие соединения с пулом
                subscribe_msg = json.dumps({"id": 1, "method": "mining.subscribe", "params": []}) + "\n"  # Сообщение подписки
                self.pool_writer.write(subscribe_msg.encode())  # Отправка подписки пулу
                await self.pool_writer.drain()  # Ожидание завершения отправки
                auth_msg = json.dumps({"id": 2, "method": "mining.authorize", "params": [WORKER_NAME, WORKER_PASSWORD]}) + "\n"  # Сообщение авторизации
                self.pool_writer.write(auth_msg.encode())  # Отправка авторизации
                await self.pool_writer.drain()  # Ожидание завершения отправки
                break  # Выход из цикла при успешном подключении
            except Exception as e:  # Обработка ошибок подключения
                logger.error(f"Pool connection failed: {e}. Reconnecting in 5 seconds...")  # Логирование ошибки
                await asyncio.sleep(5)  # Ожидание перед повторной попыткой

    async def log_shares_periodically(self):  # Периодическое логирование количества шар
        while self.running:  # Цикл работает, пока прокси активен
            await asyncio.sleep(SHARE_LOG_INTERVAL)  # Ожидание заданного интервала
            logger.info(f"Shares submitted in last {SHARE_LOG_INTERVAL//60} minutes: {self.share_count}")  # Логирование количества шар
            self.share_count = 0  # Сброс счетчика шар

    async def handle_pool_messages(self):  # Обработка сообщений от пула
        while self.running:  # Цикл работает, пока прокси активен
            try:  # Попытка обработки сообщений
                line = await self.pool_reader.readline()  # Чтение строки из пула
                if not line:  # Если строка пустая (соединение разорвано)
                    logger.warning("Pool connection closed.")  # Логирование разрыва
                    break  # Выход из цикла
                msg = json.loads(line.decode().strip())  # Преобразование строки в JSON
                logger.debug(f"Received from pool: {msg}")  # Логирование полученного сообщения

                if msg.get("method") == "mining.set_difficulty":  # Если пул устанавливает сложность
                    self.current_difficulty = msg["params"][0]  # Обновление текущей сложности
                    logger.info(f"Set difficulty: {self.current_difficulty}")  # Логирование новой сложности

                elif msg.get("result") and isinstance(msg["result"], list):  # Если это ответ на подписку
                    self.extranonce1 = msg["result"][1]  # Сохранение extranonce1
                    self.extranonce2_size = int(msg["result"][2])  # Сохранение размера extranonce2

                elif msg.get("method") == "mining.notify":  # Если пул отправил новое задание
                    self.jobs.clear()  # Очистка текущего списка заданий
                    filtered_jobs = await self.filter_jobs(msg)  # Фильтрация заданий
                    self.jobs.extend(filtered_jobs)  # Добавление отфильтрованных заданий в список
                    logger.info(f"New job queue created with {len(self.jobs)} variants")  # Логирование количества заданий

                    if self.asic_writer and self.jobs and self.last_job_time is None:  # Если есть ASIC и задания
                        self.last_job_time = datetime.now()  # Установка времени последнего задания
                        first_job = self.jobs.pop(0)  # Извлечение первого задания
                        self.asic_writer.write(json.dumps(first_job).encode() + b"\n")  # Отправка задания ASIC
                        await self.asic_writer.drain()  # Ожидание завершения отправки
                        logger.info(f"Sent initial job to ASIC: job_id={first_job['params'][0]}")  # Логирование отправки
            except Exception as e:  # Обработка ошибок
                logger.error(f"Error in pool messages: {e}")  # Логирование ошибки
                break  # Выход из цикла при ошибке

    async def handle_asic(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):  # Обработка подключения ASIC
        self.asic_writer = writer  # Сохранение объекта для записи в ASIC
        addr = writer.get_extra_info("peername")  # Получение адреса ASIC
        logger.info(f"ASIC connected from {addr}")  # Логирование подключения

        try:  # Попытка обработки сообщений от ASIC
            while self.running:  # Цикл работает, пока прокси активен
                line = await reader.readline()  # Чтение строки от ASIC
                if not line:  # Если строка пустая (соединение разорвано)
                    break  # Выход из цикла
                msg = json.loads(line.decode().strip())  # Преобразование строки в JSON
                logger.debug(f"Received from ASIC: {msg}")  # Логирование полученного сообщения

                if msg.get("method") in ["mining.subscribe", "mining.authorize"]:  # Если это подписка или авторизация
                    self.pool_writer.write(line)  # Пересылка сообщения пулу
                    await self.pool_writer.drain()  # Ожидание завершения отправки

                elif msg.get("method") == "mining.submit":  # Если ASIC отправил решение (шару)
                    self.pool_writer.write(line)  # Пересылка шары пулу
                    await self.pool_writer.drain()  # Ожидание завершения отправки
                    self.share_count += 1  # Увеличение счетчика шар
                    logger.info(f"Share submitted: {self.share_count} total")  # Логирование отправки шары

                    if self.last_job_time and (datetime.now() - self.last_job_time).seconds >= SHARE_LOG_INTERVAL:  # Если прошло достаточно времени
                        if self.jobs:  # Если есть задания в очереди
                            next_job = self.jobs.pop(0)  # Извлечение следующего задания
                            writer.write(json.dumps(next_job).encode() + b"\n")  # Отправка задания ASIC
                            await writer.drain()  # Ожидание завершения отправки
                            logger.info(f"Sent job to ASIC after interval: job_id={next_job['params'][0]}")  # Логирование отправки
                            self.last_job_time = datetime.now()  # Обновление времени последнего задания
        except Exception as e:  # Обработка ошибок
            logger.error(f"ASIC handling error: {e}")  # Логирование ошибки
        finally:  # Выполняется всегда при завершении
            writer.close()  # Закрытие соединения с ASIC
            await writer.wait_closed()  # Ожидание завершения закрытия
            logger.info(f"ASIC connection closed from {addr}")  # Логирование разрыва

    async def handle_input(self):  # Обработка ввода с клавиатуры
        loop = asyncio.get_running_loop()  # Получение текущего цикла событий
        while self.running:  # Цикл работает, пока прокси активен
            try:  # Попытка чтения ввода
                command = await loop.run_in_executor(None, input, "Enter command (stop to exit): ")  # Чтение команды из консоли
                if command.lower() == "stop":  # Если введена команда "stop"
                    self.running = False  # Остановка прокси
                    logger.info("Stopping proxy...")  # Логирование остановки
            except Exception as e:  # Обработка ошибок ввода
                logger.error(f"Input error: {e}")  # Логирование ошибки

    async def start(self):  # Запуск прокси-сервера
        try:  # Попытка запуска
            await self.connect_to_pool()  # Подключение к пулу
            asyncio.create_task(self.handle_pool_messages())  # Запуск задачи обработки сообщений от пула
            asyncio.create_task(self.log_shares_periodically())  # Запуск задачи логирования шар
            asyncio.create_task(self.handle_input())  # Запуск задачи обработки ввода

            server = await asyncio.start_server(self.handle_asic, PROXY_IP, PROXY_PORT)  # Создание сервера для ASIC
            logger.info(f"Proxy started at {PROXY_IP}:{PROXY_PORT}")  # Логирование запуска сервера
            async with server:  # Автоматическое управление сервером
                await server.serve_forever()  # Запуск сервера в бесконечном цикле
        except Exception as e:  # Обработка ошибок запуска
            logger.error(f"Startup error: {e}")  # Логирование ошибки
        finally:  # Выполняется при завершении
            if self.pool_writer:  # Если есть соединение с пулом
                self.pool_writer.close()  # Закрытие соединения
                await self.pool_writer.wait_closed()  # Ожидание завершения закрытия
            logger.info("Proxy stopped.")  # Логирование остановки

if __name__ == "__main__":  # Если скрипт запущен напрямую
    proxy = StratumProxy()  # Создание экземпляра прокси
    asyncio.run(proxy.start())  # Запуск асинхронного выполнения прокси

# Общий комментарий о работе программы:
# Этот код реализует асинхронный прокси-сервер для протокола Stratum, используемого в майнинге криптовалют.
# Он выступает посредником между ASIC-устройством (майнером) и пулом майнинга. Основные функции:
# 1. Загружает настройки из файла config.json (IP-адреса, порты, имя воркера и т.д.).
# 2. Подключается к пулу майнинга, подписывается и авторизуется.
# 3. Принимает задания от пула, фильтрует их (проверяет варианты extranonce2 на соответствие сложности) и отправляет подходящие задания ASIC.
# 4. Пересылает решения (шары) от ASIC пулу и ведет их подсчет.
# 5. Логирует работу (соединения, ошибки, шары) и позволяет остановить прокси через команду "stop".
# Программа устойчива к сбоям (переподключается к пулу при разрыве) и использует асинхронность для одновременной обработки подключений.