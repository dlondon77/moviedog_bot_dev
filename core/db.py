# core/db.py
import sqlite3
import threading
import logging
import configparser
from datetime import datetime, date, timedelta
import re
import os

# Определяем базовую директорию
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

# Загружаем конфигурацию
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

# Пути к базам данных (преобразуем относительные в абсолютные)
DB_PATH = os.path.join(BASE_DIR, config['Data']['db_path'])
MOVIES_DB_PATH = os.path.join(BASE_DIR, config['Data']['movies_db_path'])
PAYMENTS_DB_PATH = os.path.join(BASE_DIR, config['Data']['payments_db_path'])

# Создаем папки для баз данных, если их нет
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(os.path.dirname(MOVIES_DB_PATH), exist_ok=True)
os.makedirs(os.path.dirname(PAYMENTS_DB_PATH), exist_ok=True)

# Лок для потокобезопасности
DB_LOCK = threading.Lock()

# Настраиваем логгер
logger = logging.getLogger('core.db')

# ==================== ОЧИСТКА ТЕКСТА ====================

def clean_text(text, for_sql=False):
    """
    Универсальная функция очистки текста.
    
    Параметры:
    - for_sql: если True, выполняет более строгую очистку для использования в SQL-запросах
    
    Возвращает:
    - Очищенную строку или None, если входной текст был None
    """
    if text is None:
        return None
        
    # Преобразуем в строку на случай, если передано число или другой тип
    text = str(text).strip()
    
    # Всегда удаляем markdown-разметку
    text = re.sub(r'[\*\_\`]', '', text)
    
    if for_sql:
        # Строгая очистка для SQL: оставляем только буквы, цифры, пробелы и дефисы
        text = re.sub(r"[^\w\sа-яА-ЯёЁ-]", "", text, flags=re.UNICODE)
    
    return text.strip()

# ==================== ПОДКЛЮЧЕНИЯ К БАЗАМ ====================

def get_movies_db_connection():
    """Возвращает соединение с movies.db с зарегистрированной функцией clean_text"""
    conn = sqlite3.connect(MOVIES_DB_PATH)
    conn.create_function("clean_text", 1, clean_text)
    return conn

def get_opinions_db_connection():
    """Возвращает соединение с opinions.db"""
    conn = sqlite3.connect(DB_PATH)
    return conn

def get_payments_db_connection():
    """Возвращает соединение с payments.db"""
    conn = sqlite3.connect(PAYMENTS_DB_PATH)
    return conn

# ==================== ИНИЦИАЛИЗАЦИЯ ВСЕХ БАЗ ====================

def init_db():
    """Полная инициализация всех баз данных"""
    with DB_LOCK:
        # ================== 1. Инициализация opinions.db ==================
        opinions_conn = get_opinions_db_connection()
        try:
            # Оптимизация работы SQLite
            opinions_conn.execute("PRAGMA journal_mode=WAL;")
            opinions_conn.execute("PRAGMA synchronous=NORMAL;")
            opinions_conn.execute("PRAGMA busy_timeout=5000;")
            
            cursor = opinions_conn.cursor()
            
            # Таблица мнений о фильмах
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS movie_opinions (
                    movie_id INTEGER PRIMARY KEY,
                    short_opinion TEXT,
                    full_opinion TEXT,
                    mood_tags TEXT,
                    atmosphere_tags TEXT,
                    created_at TEXT
                )
            ''')
            
            # Таблица пользователей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    registered_at TEXT,
                    platform TEXT DEFAULT 'telegram'
                )
            ''')
            
            # ===== МИГРАЦИЯ: добавляем колонку platform =====
            # Проверяем, есть ли колонка platform
            cursor.execute("PRAGMA table_info(users)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'platform' not in columns:
                logger.info("🔄 Добавляем колонку platform в таблицу users...")
                cursor.execute("ALTER TABLE users ADD COLUMN platform TEXT DEFAULT 'telegram'")
                # Для существующих пользователей проставляем 'telegram'
                cursor.execute("UPDATE users SET platform = 'telegram' WHERE platform IS NULL")
                logger.info("✅ Миграция завершена")
            
            # Таблица лимитов пользователей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_limits (
                    user_id INTEGER PRIMARY KEY,
                    opinion_limit INTEGER DEFAULT 3,
                    custom_query_limit INTEGER DEFAULT 10,
                    custom_retry_limit INTEGER DEFAULT 30,
                    kinopoisk_query_limit INTEGER DEFAULT 50,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            
            # Таблица статистики пользователей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    date TEXT,
                    opinion_count INTEGER DEFAULT 0,
                    custom_query_count INTEGER DEFAULT 0,
                    custom_retry_count INTEGER DEFAULT 0,
                    kinopoisk_query_count INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    UNIQUE (user_id, date)
                )
            ''')
            
            # Таблица пользовательских мнений
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_movie_opinions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    movie_id INTEGER,
                    created_at TEXT,
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    FOREIGN KEY (movie_id) REFERENCES movie_opinions (movie_id)
                )
            ''')
            
            # Таблица обратной связи
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS feedback (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    type INTEGER,  -- 1: ошибка, 2: отзыв
                    movie_id TEXT,
                    message TEXT,
                    status TEXT DEFAULT 'new',  -- new, in_progress, resolved, archive
                    admin_comment TEXT,
                    created_at TEXT,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            
            # Таблица тарифов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tariff_plans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    opinion_limit INTEGER NOT NULL,
                    regeneration_limit INTEGER NOT NULL,
                    custom_query_limit INTEGER DEFAULT 10,
                    custom_retry_limit INTEGER DEFAULT 30,
                    kinopoisk_query_limit INTEGER DEFAULT 50,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица пользовательских подписок
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_subscriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    tariff_id INTEGER NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    is_active BOOLEAN DEFAULT 1,
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    FOREIGN KEY (tariff_id) REFERENCES tariff_plans (id)
                )
            ''')
            
            # Таблица истории подписок
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS subscription_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    tariff_id INTEGER NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    changed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    FOREIGN KEY (tariff_id) REFERENCES tariff_plans (id)
                )
            ''')
            
            # Таблица статистики пользователей (новая)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    date TEXT NOT NULL,
                    opinion_count INTEGER DEFAULT 0,
                    regeneration_count INTEGER DEFAULT 0,
                    custom_query_count INTEGER DEFAULT 0,
                    custom_retry_count INTEGER DEFAULT 0,
                    kinopoisk_query_count INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    UNIQUE (user_id, date)
                )
            ''')
                        
            opinions_conn.commit()
            logger.info("✅ База opinions.db успешно инициализирована")
            
        except sqlite3.Error as e:
            logger.error(f"❌ Ошибка инициализации opinions.db: {e}")
            raise
        finally:
            opinions_conn.close()         

        # ================== 2. Инициализация movies.db ==================
        movies_conn = get_movies_db_connection()
        try:
            # Оптимизация работы SQLite
            movies_conn.execute("PRAGMA journal_mode=WAL;")
            movies_conn.execute("PRAGMA synchronous=NORMAL;")
            movies_conn.execute("PRAGMA busy_timeout=5000;")
            
            cursor = movies_conn.cursor()
            
            # Основные таблицы
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS movies (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    enName TEXT,
                    year INTEGER,
                    description TEXT,
                    rating REAL,
                    movie_type TEXT,
                    poster_url TEXT,
                    premiere_russia TEXT,
                    premiere_world TEXT,
                    await_count INTEGER,
                    is_new_release BOOLEAN DEFAULT 0
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS genres (
                    movie_id INTEGER,
                    genre TEXT,
                    FOREIGN KEY (movie_id) REFERENCES movies (id),
                    PRIMARY KEY (movie_id, genre)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS countries (
                    movie_id INTEGER,
                    country TEXT,
                    FOREIGN KEY (movie_id) REFERENCES movies (id),
                    PRIMARY KEY (movie_id, country)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS actors (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    enName TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS movie_actors (
                    movie_id INTEGER,
                    actor_id INTEGER,
                    FOREIGN KEY (movie_id) REFERENCES movies (id),
                    FOREIGN KEY (actor_id) REFERENCES actors (id),
                    PRIMARY KEY (movie_id, actor_id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS directors (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    enName TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS movie_directors (
                    movie_id INTEGER,
                    director_id INTEGER,
                    FOREIGN KEY (movie_id) REFERENCES movies (id),
                    FOREIGN KEY (director_id) REFERENCES directors (id),
                    PRIMARY KEY (movie_id, director_id)
                )
            ''')
                     
            movies_conn.commit()
            logger.info("✅ База movies.db успешно инициализирована")
      
        except sqlite3.Error as e:
            logger.error(f"❌ Ошибка инициализации movies.db: {e}")
            raise
        finally:
            movies_conn.close()
        
        # ================== 3. Инициализация payments.db ==================
        payments_conn = get_payments_db_connection()
        try:
            payments_conn.execute("PRAGMA journal_mode=WAL;")
            payments_conn.execute("PRAGMA synchronous=NORMAL;")
            
            cursor = payments_conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    payment_id TEXT NOT NULL,
                    order_id TEXT NOT NULL UNIQUE,
                    amount INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    description TEXT,
                    payment_url TEXT,  
                    user_email TEXT,   
                    user_phone TEXT,   
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute("PRAGMA table_info(payments)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'user_phone' not in columns:
                cursor.execute("ALTER TABLE payments ADD COLUMN user_phone TEXT")
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_payments_user_id ON payments(user_id);
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status);
            ''')
            payments_conn.commit()
            logger.info("✅ База payments.db успешно инициализирована")
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации payments.db: {e}")
            raise
        finally:
            payments_conn.close()
