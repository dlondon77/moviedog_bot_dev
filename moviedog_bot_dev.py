import asyncio
import time
import hashlib
import requests
import configparser
import os
import logging
import json
import sqlite3
import re
from datetime import datetime, date, timedelta, timezone
import random
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    CallbackQueryHandler,
)
import httpx
from telegram.request import HTTPXRequest

# ==================== ПОЛНОЕ ОТКЛЮЧЕНИЕ ПРОКСИ ====================
# Удаляем все возможные переменные прокси
for env_var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(env_var, None)

# Создаем кастомный HTTP клиент без прокси
custom_async_client = httpx.AsyncClient(
    timeout=httpx.Timeout(30.0),
    limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
    follow_redirects=True
)

# Создаем кастомный HTTPXRequest с нашим клиентом
custom_request = HTTPXRequest(
    connection_pool_size=1,
    connect_timeout=30.0,
    read_timeout=30.0,
    write_timeout=30.0,
    pool_timeout=30.0
)
# Подменяем внутренний клиент
custom_request._client = custom_async_client

# ==================== ИМПОРТЫ CORE ====================
from core import admin, db, user, movie

# ==================== КОНФИГУРАЦИЯ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

DB_PATH = os.path.join(BASE_DIR, config['Data']['db_path'])
MOVIES_DB_PATH = os.path.join(BASE_DIR, config['Data']['movies_db_path'])
LOG_PATH = os.path.join(BASE_DIR, config['Logs']['log_path'])
PAYMENTS_DB_PATH = os.path.join(BASE_DIR, config['Data']['payments_db_path'])

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN') or config['Telegram']['token']
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY') or config['OpenAI']['api_key']
OPENAI_BASE_URL = config['OpenAI']['base_url']

if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_TOKEN не найден!")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY не найден!")

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
os.makedirs(os.path.dirname(PAYMENTS_DB_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

payments_logger = logging.getLogger('payments')
payments_logger.setLevel(logging.INFO)
payments_handler = logging.FileHandler(PAYMENTS_DB_PATH, encoding='utf-8')
payments_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
payments_logger.addHandler(payments_handler)

# ==================== ИНИЦИАЛИЗАЦИЯ OPENAI ====================
from openai import OpenAI

try:
    sync_client = httpx.Client(
        timeout=30.0,
        follow_redirects=True
    )
    
    client = OpenAI(
        api_key=OPENAI_API_KEY,
        base_url=OPENAI_BASE_URL,
        http_client=sync_client
    )
    logger.info("✅ OpenAI клиент успешно инициализирован")
except Exception as e:
    logger.error(f"❌ Ошибка инициализации OpenAI: {e}")
    client = None

# ==================== ИНИЦИАЛИЗАЦИЯ БАЗ ДАННЫХ ====================
try:
    db.init_db()
    logger.info("✅ Базы данных успешно инициализированы")
except Exception as e:
    logger.error(f"❌ Ошибка инициализации баз данных: {e}")

# ==================== ФУНКЦИИ ДЛЯ РАБОТЫ С МНЕНИЯМИ ====================

def get_opinion(movie_id):
    """
    Безопасное получение мнения о фильме.
    Возвращает None, если мнение не найдено.
    """
    with db.DB_LOCK:
        conn = db.get_opinions_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT * FROM movie_opinions WHERE movie_id = ?', 
                (int(movie_id),)  # Явное приведение к int для защиты от SQL-инъекций
            )
            return cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Ошибка получения мнения (movie_id={movie_id}): {e}")
            return None
        finally:
            conn.close()


def save_opinion(movie_id, short_opinion, full_opinion, mood_tags, atmosphere_tags):
    """Безопасное сохранение мнения с проверкой целостности"""
    with db.DB_LOCK:
        conn = db.get_opinions_db_connection()
        try:
            cursor = conn.cursor()
            
            # Проверяем целостность перед записью
            integrity = cursor.execute("PRAGMA quick_check;").fetchone()
            if integrity[0] != "ok":
                raise ValueError(f"Целостность БД нарушена: {integrity[0]}")
            
            # Сохраняем мнение
            cursor.execute('''
                INSERT OR REPLACE INTO movie_opinions 
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                int(movie_id),
                str(short_opinion),
                str(full_opinion),
                str(mood_tags),
                str(atmosphere_tags),
                datetime.now().isoformat()
            ))
            
            conn.commit()
            logger.info(f"Сохранено мнение для movie_id={movie_id}")
            
        except sqlite3.Error as e:
            logger.error(f"Ошибка сохранения мнения (movie_id={movie_id}): {e}")
            conn.rollback()
            raise
        finally:
            conn.close()


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def log_user_action(update: Update, action: str):
    user_obj = update.message.from_user if update.message else update.callback_query.from_user
    logger.info(f"Пользователь {user_obj.username} (ID: {user_obj.id}) выполнил действие: {action}")


def get_donate_button():
    """Создает инлайн-кнопку для пожертвований"""
    return InlineKeyboardButton("На корм и развитие навыков", callback_data="donate")


# ==================== ПЛАТЕЖНЫЕ ФУНКЦИИ ====================

async def ask_for_email(update: Update, context: ContextTypes.DEFAULT_TYPE, amount: int):
    """Запрашивает email и телефон у пользователя перед созданием платежа"""
    context.user_data['donate_amount'] = amount
    context.user_data['donate_stage'] = 'awaiting_email'
    
    keyboard = [
        [InlineKeyboardButton("Пропустить", callback_data="skip_email")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="🐾 Для дополнительной верификации платежа укажи свой email (можно пропустить):",
        reply_markup=reply_markup
    )


async def ask_for_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрашивает телефон пользователя в обязательном порядке"""
    context.user_data['donate_stage'] = 'awaiting_phone'
    
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="📱 Для подтверждения операции укажи свой телефон в формате +79000000000 (не используется для рассылок и не передается третьим лицам):"
    )


async def init_payment(user_id: int, amount: int, description: str, user_email: str, user_phone: str = None):
    """Инициализация платежа через Tinkoff API"""
    try:
        current_time = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
        order_id = f"donate_{user_id}_{int(time.time())}"
        amount_kop = amount * 100
        
        # Формируем чек с телефоном, если он указан
        receipt = {
            "Email": user_email,
            "Phone": user_phone if user_phone else "+79000000000",
            "Taxation": "usn_income",
            "Items": [{
                "Name": description,
                "Price": amount_kop,
                "Quantity": 1,
                "Amount": amount_kop,
                "Tax": "none"
            }]
        }
        
        params = {
            "TerminalKey": config['Tinkoff']['terminal_key'],
            "Amount": amount_kop,
            "OrderId": order_id,
            "Description": description,
            "Receipt": receipt,
            "Token": generate_token('Init', {
                "Amount": amount_kop,
                "OrderId": order_id,
                "Description": description
            })
        }
        
        payments_logger.info(f"Initializing payment for user {user_id}, amount: {amount} rub")
        
        response = requests.post(
            config['Tinkoff']['init_url'],
            json=params,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        
        if data.get('Success'):
            conn = sqlite3.connect(config['Data']['payments_db_path'])
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO payments 
                (user_id, payment_id, order_id, amount, status, description, user_email, user_phone, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                user_id, 
                data['PaymentId'], 
                order_id, 
                amount, 
                'NEW', 
                description,
                user_email,
                user_phone,
                current_time,
                current_time
            ))
            conn.commit()
            conn.close()
            
            payments_logger.info(f"Payment initialized successfully. PaymentId: {data['PaymentId']}")
            return data
            
        payments_logger.error(f"Payment initialization failed. Response: {data}")
        return None
        
    except Exception as e:
        payments_logger.error(f"Payment initialization error: {str(e)}", exc_info=True)
        return None


def generate_token(method: str, params: dict) -> str:
    """Генерация токена для Tinkoff API согласно документации"""
    try:
        # Читаем пароль с отключенной интерполяцией
        temp_config = configparser.ConfigParser(interpolation=None)
        temp_config.read('/volume1/homes/Dima/tgbots/moviedog/dev/config/config.ini')
        password = temp_config['Tinkoff']['password']
        
        sign_params = {
            'TerminalKey': config['Tinkoff']['terminal_key'],
            'Password': password
        }
        
        if method == 'Init':
            sign_params.update({
                'Amount': str(params['Amount']),
                'OrderId': params['OrderId'],
                'Description': params.get('Description', '')
            })
        elif method == 'GetState':
            sign_params.update({
                'PaymentId': params['PaymentId']
            })
        
        # Сортируем параметры по алфавиту
        sorted_params = sorted(sign_params.items(), key=lambda x: x[0])
        
        # Объединяем значения в одну строку
        token_str = ''.join(str(v) for _, v in sorted_params)
        
        # Логируем только для отладки (без пароля)
        logger.debug(f"Generating token from: TerminalKey + Password + {params}")
        
        # Возвращаем SHA256 хеш
        return hashlib.sha256(token_str.encode('utf-8')).hexdigest()
        
    except Exception as e:
        logger.error(f"Ошибка генерации токена: {str(e)}")
        raise


# ==================== КОМАНДЫ БОТА ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    log_user_action(update, "/start")
    
    # Регистрируем пользователя
    user_obj = update.message.from_user
    user.register_user(
        user_id=user_obj.id,
        username=user_obj.username,
        first_name=user_obj.first_name,
        last_name=user_obj.last_name
    )
    
    # Получаем URL изображения и канала из конфига
    start_image_url = config['Images']['start_image_url']
    channel_url = config['Channel']['channel_url']
    
    # Создаем клавиатуру с кнопкой пожертвований
    keyboard = [
        [InlineKeyboardButton("На корм и развитие навыков", callback_data="donate")],
        [InlineKeyboardButton("Мой канал", url=channel_url)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Отправляем изображение с текстом
    await update.message.reply_photo(
        photo=start_image_url,
        caption=(
            "🐾 Гав! Я - КиноИщейка! Добро пожаловать в мир кино! 🎬\n\n"
            "Я помогу тебе найти фильмы, сериалы и мультфильмы на Кинопоиске, которые ты точно полюбишь.\n\n"
            "Вот какие команды я знаю:\n\n"
            "/about — кратко рассказать о себе\n"
            "/random — найти следы случайного фильма\n"
            "/search — найти отборные фильмы по ключевым ориентировкам в названии\n"
            "/premiers — учуять свежие ожидаемые премьеры\n"
            "/person — найти фильмы по запахам актеров или режиссеров\n"
            "/faq — ответить на частые вопросы\n"
            "/feedback — понять неточности и пути развития\n\n"
            "Нашел фильм? — Тогда жми кнопку <b>Мнение КиноИщейки</b> и я расскажу тебе о смысле фильма, его настроении и атмосфере, укажу на плюсы и минусы и поставлю оценку\n\n"
            "<b>Мои команды всегда доступны по кнопке Меню</b>\n(в левом нижнем углу)\n👇🏻\n\n"
            "Выбирай команду и побежали! 🍿\n\n"
            f"✍🏻 <a href='{channel_url}'>Подписаться на мой канал</a>\n\n"
        ),
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    log_user_action(update, "/about")
    
    # Получаем URL изображения и канала из конфига
    about_image_url = config['Images']['about_image_url']
    channel_url = config['Channel']['channel_url']
    
    # Создаем клавиатуру с кнопкой пожертвований
    keyboard = [
        [InlineKeyboardButton("На корм и развитие навыков", callback_data="donate")],
        [InlineKeyboardButton("Мой канал", url=channel_url)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Текст описания с HTML-разметкой
    about_text = (
        "🎬 <b>КиноИщейка — ваш пушистый гид в мире кино!</b>\n\n"
        "🐾 Я — не просто бот, а настоящий киногурман с собачьим нюхом на отличные фильмы! "
        "Моя миссия — находить для вас самые увлекательные, трогательные и неожиданные кинокартины.\n\n"
        "Со мной вы сможете открывать новые фильмы, получать мои мнения о просмотренных фильмах "
        "и узнавать много интересного о кино.\n\n"
        "Я обожаю анализировать сюжеты, разбираться в атмосфере фильмов и делиться своими находками — "
        "с капелькой юмора и собачьей искренности!\n\n"
        "<i>P.S. Виртуальные поглаживания по голове приветствуются!</i> 🐾\n\n"
        f"✍🏻 <a href='{channel_url}'>Подписаться на мой канал</a>\n\n"
    )
    
    # Отправляем фото с подписью
    await update.message.reply_photo(
        photo=about_image_url,
        caption=about_text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def random_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    log_user_action(update, "/random")
    
    movie_data = movie.get_random_movie_from_db(min_rating=7.0, is_new_only=False)
    
    if not movie_data:
        await update.message.reply_text("Фильмы не найдены. Попробуй позже.")
        return
    
    movie_details = movie.get_movie_details(movie_data['id'])
    card, reply_markup = movie.format_movie_card(movie_details)
    
    if card:
        await update.message.reply_text(card, parse_mode='HTML', reply_markup=reply_markup)
    else:
        await update.message.reply_text("Гав! Кажется, я перепутала следы... Не могу найти информацию об этом фильме.")


async def search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    log_user_action(update, "/search")
       
    await update.message.reply_text(
        "🐾 Введи название фильма и помчу искать его среди отборного кино с рейтингом выше 5 баллов и новинок!\n\n"
    )


async def person(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    context.user_data['is_person_search'] = True
    log_user_action(update, "/person")
       
    await update.message.reply_text(
        "🐾 Введи имя актера или режиссера, и я найду фильмы с их участием и рейтингом выше 5 и новинок! Мой нюх на звезд кино просто потрясающий!"
    )


async def premiers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    log_user_action(update, "/premiers")
    
    # Заглушка для долгого поиска
    await update.message.reply_text(
        "🐕‍🦺💨 Запускаю сканер премьер! Постараюсь побыстрее, обычно это занимает не более 30 секунд!"
    )
    
    movies_list = movie.get_premier_movies_from_db()
    
    if not movies_list:
        await update.message.reply_text("🐾 Не нашла ничего свеженького...")
        return
    
    if len(movies_list) == 100:
        await update.message.reply_text(
            "🐾 Ого-го! Столько премьер, что даже моя миска с попкорном переполнилась! \n\n"
            "Покажу только 100 премьер стартовавших неделю назад. Приготовься к киномарафону!"
        )
        movies_list = movies_list[:100]
    
    context.user_data['movies'] = movies_list
    context.user_data['current_index'] = 0
    context.user_data['is_premiers'] = True

    await update.message.reply_text(f"Нашла вон сколько фильмов: {len(movies_list)}")
    await show_movies(update, context, is_premiers=True)


async def faq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_faq_menu(update, context)


async def show_faq_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("Как найти фильм?", callback_data="faq_search")],
        [InlineKeyboardButton("Как узнать мнение о фильме?", callback_data="faq_opinion")],
        [InlineKeyboardButton("Какие есть лимиты?", callback_data="faq_limits")],
        [InlineKeyboardButton("Как оплатить донат?", callback_data="faq_donate")],
        [InlineKeyboardButton("Как предложить улучшение?", callback_data="faq_suggest")],
        [InlineKeyboardButton("Закрыть", callback_data="faq_close")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "🐾 <b>Часто задаваемые вопросы:</b>\n\nВыбери вопрос из списка:",
            parse_mode='HTML',
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            "🐾 <b>Часто задаваемые вопросы:</b>\n\nВыбери вопрос из списка:",
            parse_mode='HTML',
            reply_markup=reply_markup
        )


# ==================== ПОКАЗ ФИЛЬМОВ С ПАГИНАЦИЕЙ ====================

async def show_movies(update: Update, context: ContextTypes.DEFAULT_TYPE, is_premiers=False):
    movies_list = context.user_data.get('movies', [])
    current_index = context.user_data.get('current_index', 0)
    query_text = context.user_data.get('query', None)
    is_person_search = context.user_data.get('is_person_search', False)

    message = update.message if update.message else update.callback_query.message

    # Показываем по 5 фильмов за раз
    end_index = min(current_index + 5, len(movies_list))
    shown_count = 0
    
    for movie_data in movies_list[current_index:end_index]:
        card, reply_markup = movie.format_movie_card(movie_data, is_premiers, query_text, is_person_search)
        if card:
            try:
                await message.reply_text(card, parse_mode='HTML', reply_markup=reply_markup)
                shown_count += 1
            except Exception as e:
                logger.error(f"Ошибка показа фильма: {e}")
                title = movie_data.get('name', 'Неизвестный фильм')
                await message.reply_text(f"🐾 Ошибка при показе фильма: {title}")

    # Обновляем индекс
    new_index = current_index + shown_count
    context.user_data['current_index'] = new_index

    # Если есть еще фильмы, предлагаем продолжить
    if new_index < len(movies_list):
        keyboard = [
            [InlineKeyboardButton("Да", callback_data="continue_search")],
            [InlineKeyboardButton("Нет", callback_data="stop_search")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await message.reply_text(f"🐾 Лови {new_index} из {len(movies_list)} ☝🏻")
        await message.reply_text("Дальше бежим?", reply_markup=reply_markup)
    else:
        await message.reply_text(f"🐾 Лови {len(movies_list)} из {len(movies_list)} ☝🏻")
        await message.reply_text("🐾 Поиски завершены. Если что - я всегда рядом!\n\nЕсли ты не нашел фильм, то возможно его рейтинг не набрал еще 5 баллов и его нет среди новинок.\n\nЕсли что-то не так - сообщи мне через /feedback")
        context.user_data.clear()


# ==================== ОБРАБОТКА ТЕКСТОВЫХ СООБЩЕНИЙ ====================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка обычных текстовых сообщений (поиск)"""
    if context.user_data.get('is_person_search', False):
        await handle_person_search(update, context)
        return
    
    await handle_movie_search(update, context)


async def handle_movie_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip().lower()
    log_user_action(update, f"Поиск по ключевому слову: {query}")

    if len(query) < 2:
        await update.message.reply_text("🐾 Введи не менее 2 символов для поиска.")
        return

    await update.message.reply_text(
        "🔍 <i>Гав-гав! Взяла след по вашему запросу...</i>\n\n"
        "Сейчас посчитаю, сколько нашлось фильмов...",
        parse_mode='HTML'
    )

    # Сохраняем запрос в контекст (ЭТО ВАЖНО!)
    context.user_data['query'] = query

    # Получаем количество фильмов и флаг has_more
    total_count, has_more = movie.search_movies_with_filters(query, filters=None, count_only=True)
    
    if total_count == 0:
        await update.message.reply_text(
            "🐾 Не нашла фильмов. Попробуй уточнить название."
        )
        return
    
    # Получаем полный список для последующего показа
    full_list = movie.search_movies_with_filters(query, filters=None, count_only=False)
    context.user_data['full_movies_list'] = full_list
    
    # Показываем интерфейс с фильтрами
    text = f"🔍 <b>Поиск: {query}</b>\n\n"
    if has_more:
        text += f"Найдено фильмов: <b>>{total_count}</b>\n\n"
    else:
        text += f"Найдено фильмов: <b>{total_count}</b>\n\n"
    
    text += "Используй фильтры для уточнения, затем нажми 'Показать карточки'"
    
    filter_keyboard = movie.format_filter_keyboard(query, None, total_count, has_more)
    
    await update.message.reply_text(
        text, 
        parse_mode='HTML', 
        reply_markup=filter_keyboard,
        disable_web_page_preview=True
    )

async def handle_person_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip().lower()
    log_user_action(update, f"Поиск по персонам: {query}")

    if len(query) < 2:
        await update.message.reply_text("🐾 Введи не менее 2 символов для поиска.")
        return

    await update.message.reply_text(
        "🕵️‍♀️ <i>Мой собачий нюх уже работает на полную мощность...</i>\n\n"
        "Это может занять до 30 секунд - хорошие фильмы любят прятаться!\n"
        "Как только что-то найду - сразу дам голос!",
        parse_mode='HTML'
    )

    movies_list = movie.search_movies_by_person_in_db(query, min_rating=0.0, max_rating=10.0)
    
    if not movies_list:
        await update.message.reply_text(
            "🐾 Не нашла фильмов с этой персоной среди рейтинга от 5 баллов и новинок. Попробуй уточнить запрос."
        )
        return
    
    if len(movies_list) == 100:
        await update.message.reply_text(
            "🐾 Вау! Эта звезда засветилась в стольких фильмах, что у меня лапы устали их считать!\n\n "
            "Покажу только топ-100 по рейтингу. Может уточнишь имя и фамилию?"
        )
        movies_list = movies_list[:100]
    
    context.user_data['movies'] = movies_list
    context.user_data['current_index'] = 0
    context.user_data['query'] = query
    context.user_data['is_person_search'] = True

    await update.message.reply_text(f"Нашла вон сколько фильмов: {len(movies_list)}")
    await show_movies(update, context)


# ==================== AI МНЕНИЕ О ФИЛЬМЕ ====================

async def handle_ai_message(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                           movie_id=None, year=None, force_regenerate=False, 
                           regeneration_reason=None, mock_callback_query=None):
    
    # Определяем источник вызова и получаем данные пользователя
    if mock_callback_query:
        user_obj = mock_callback_query.from_user
        chat_id = update.effective_chat.id
        is_callback = False
        message = update.message
    elif update.callback_query:
        user_obj = update.callback_query.from_user
        chat_id = update.callback_query.message.chat_id
        is_callback = True
        message = update.callback_query.message
    else:
        user_obj = update.message.from_user if update.message else None
        chat_id = update.effective_chat.id
        is_callback = False
        message = update.message
    
    if not user_obj:
        logger.error("Не удалось определить пользователя")
        await context.bot.send_message(
            chat_id=chat_id,
            text="Гав! Что-то пошло не так. Попробуй еще раз!"
        )
        return
    
    today = date.today().isoformat()
    
    user.register_user(
        user_id=user_obj.id,
        username=user_obj.username,
        first_name=user_obj.first_name,
        last_name=user_obj.last_name
    )
    
    # Проверяем лимиты
    limits = user.get_user_limits(user_obj.id)
    stats = user.get_user_stats(user_obj.id, today)
    
    # Проверяем срок действия тарифа
    try:
        tariff_end_date = datetime.fromisoformat(limits['tariff_end_date'])
        if datetime.now() > tariff_end_date:
            await context.bot.send_message(
                chat_id=chat_id,
                text="🐾 Ваш тарифный план истек. Пожалуйста, выберите новый тариф."
            )
            return
    except:
        pass

    # Получаем полную информацию о фильме из базы
    movie_details = movie.get_movie_details(int(movie_id))
    if not movie_details:
        await context.bot.send_message(
            chat_id=chat_id,
            text="Гав! Не могу найти этот фильм, кажется я запуталась в проводах."
        )
        return
    
    # Извлекаем все данные для промта
    title = movie_details.get('name', 'Без названия')
    year = movie_details.get('year', '')
    
    # Страны
    countries = movie_details.get('countries', [])
    countries_str = ', '.join(countries) if countries else 'неизвестно'
    
    # Жанры
    genres = movie_details.get('genres', [])
    genres_str = ', '.join(genres) if genres else 'неизвестно'
    
    # Режиссеры
    directors_list = movie_details.get('directors', [])
    if directors_list:
        director_names = []
        for director in directors_list:
            name = director.get('name') or director.get('enName')
            if name:
                director_names.append(name)
        directors_str = ', '.join(director_names)
    else:
        directors_str = 'неизвестен'
    
    # Актеры (первые 7)
    actors_list = movie_details.get('actors', [])[:7]
    if actors_list:
        actor_names = []
        for actor in actors_list:
            name = actor.get('name') or actor.get('enName')
            if name:
                actor_names.append(name)
        actors_str = '\n'.join([f"• {name}" for name in actor_names])
    else:
        actors_str = 'не указаны'
    
    # Рейтинг
    rating = movie_details.get('rating', 0)
    
    # Описание
    description = movie_details.get('description', 'Описание отсутствует')
    if description and len(description) > 800:
        description = description[:800] + '...'
    
    # Проверяем существующее мнение
    existing_opinion = get_opinion(movie_id) if movie_id else None
    
    # Если это не регенерация и есть существующее мнение - показываем его
    if existing_opinion and not force_regenerate:
        _, short_opinion, full_opinion, mood_tags, atmosphere_tags, created_at = existing_opinion
        kp_url = f"https://www.kinopoisk.ru/film/{movie_id}/"
        title_with_link = f"<a href='{kp_url}'>{title}</a> ({year})"
        
        # Фиксируем просмотр мнения
        user.record_user_opinion(user_obj.id, movie_id)
        
        # Добавляем кнопки
        keyboard = [
            [InlineKeyboardButton("Получить свежий взгляд", callback_data=f"regenerate:{movie_id}:{year}")],
            [InlineKeyboardButton("Сообщить о неточности", callback_data=f"report_error:{movie_id}")],
            [InlineKeyboardButton("На корм и развитие навыков", callback_data="donate")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"Я уже смотрела {title_with_link}, и вот что думаю:\n\n{full_opinion}\n\n🐾",
            parse_mode='HTML',
            reply_markup=reply_markup
        )
        return
    
    # Проверяем лимит на мнения (для новых мнений)
    if not force_regenerate and stats['opinion_count'] >= limits['opinion_limit']:
        await context.bot.send_message(
            chat_id=chat_id,
            text="🐾 Гав! Сегодня я уже высказала максимальное количество мнений.\n "
                 f"Ты использовал:\n- {stats['opinion_count']} из {limits['opinion_limit']} доступных мнений.\n\n"
                 f"- {stats['regeneration_count']} из {limits['regeneration_limit']} доступных свежих взглядов.\n"
                 "Лимиты обновятся в полночь и я снова буду готова помогать! 🍿"
        )
        return
    
    # Проверяем лимит на свежие взгляды (для регенерации)
    if force_regenerate and stats['regeneration_count'] >= limits['regeneration_limit']:
        await context.bot.send_message(
            chat_id=chat_id,
            text="🐾 Гав! Сегодня я уже сформировала максимальное количество свежих взглядов.\n "
                 f"У тебя использовано:\n- {stats['regeneration_count']} из {limits['regeneration_limit']} доступных свежих взглядов.\n"
                 f"- {stats['opinion_count']} из {limits['opinion_limit']} доступных мнений.\n\n"
                 "Лимиты обновятся в полночь и я снова буду готова помогать! 🍿"
        )
        return
    
    # Отправляем сообщение о начале генерации
    await context.bot.send_chat_action(chat_id=chat_id, action="typing")
    await context.bot.send_message(
        chat_id=chat_id,
        text="Смотрю фильм в ускоренном режиме..."
    )
    
    try:
        if is_callback:
            await update.callback_query.message.chat.send_action(action="typing")
        else:
            await context.bot.send_chat_action(chat_id=chat_id, action="typing")
        
        # Формируем промт со всеми данными
        prompt = f"""Ты — КиноИщейка, собака-девочка, кинокритик с отличным чутьем на хорошее кино. Ты смотришь фильмы и делишься своим мнением с юмором и энтузиазмом. Говори о себе в женском роде.

Информация о фильме:
🎬 Название: {title} ({year})
🌍 Страна: {countries_str}
🎭 Жанр: {genres_str}
🎥 Режиссер: {directors_str}
⭐ Рейтинг Кинопоиска: {rating}
👥 В главных ролях:
{actors_str}

📝 Сюжет:
{description}

Требования к ответу:
1. Объем: 10-12 предложений
2. Без markdown-разметки
3. Только обычный текст
4. Разделяй части мнения переносами строк
5. Добавь собачий юмор
6. Говори о себе в женском роде
7. НЕ используй вводные фразы типа "Я посмотрела фильм и вот что думаю" - сразу начинай с содержательной части
8. Не благодари за замечания и не упоминай, что это исправленная версия - просто напиши новое мнение

Расскажи о:
- Настроении и смысле фильма
- Наградах (с учетом страны производства, если знаешь точно, а если нет - просто не упоминай, не выдумывай!)
- Особенностях
- Почему стоит посмотреть
- Плюсах и минусах"""

        # Если есть причина для регенерации, добавляем её в промт
        if regeneration_reason:
            prompt += f"""

Пользователь указал на ошибку в моем предыдущем мнении: "{regeneration_reason}"
Учти это в новом мнении, но не упоминай сам факт исправления."""

        # Если есть существующее мнение и мы делаем регенерацию, добавляем его как контекст
        if existing_opinion and force_regenerate:
            _, _, old_full_opinion, _, _, _ = existing_opinion
            prompt += f"""

Мое предыдущее мнение (для контекста, чтобы ты понимала, о чем уже говорилось):
{old_full_opinion[:500]}..."""

        prompt += """

В конце обязательно добавь:
Оценка: от 5 до 10 (краткий комментарий почему)

После оценки добавь:
Настроение: 5 хэштегов (например #Радость #Грусть)
Атмосфера: 5 хэштегов (например #Мрачность #Яркость)"""

        # Логируем промт для отладки
        logger.info(f"Отправляю промт для фильма {title} ({year}), длина: {len(prompt)} символов")
        if regeneration_reason:
            logger.info(f"Причина регенерации: {regeneration_reason}")

        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {
                    "role": "system", 
                    "content": "Ты — КиноИщейка, собака-девочка, кинокритик. Твои ответы должны быть дружелюбными, с юмором, но при этом информативными. Обязательно используй женский род: 'я посмотрела', 'мне понравилось', 'я нашла' и т.д."
                },
                {
                    "role": "user", 
                    "content": prompt
                }
            ],
            stream=False,
            timeout=60
        )

        full_response = db.clean_text(response.choices[0].message.content)
        
        # Парсим ответ AI
        short_opinion = ""
        mood_tags = ""
        atmosphere_tags = ""
                     
        for part in full_response.split('\n'):
            if part.startswith("Оценка:"):
                short_opinion = db.clean_text(part)
            elif part.startswith("Настроение:"):
                mood_tags = db.clean_text(part.replace("Настроение:", ""))
            elif part.startswith("Атмосфера:"):
                atmosphere_tags = db.clean_text(part.replace("Атмосфера:", ""))
        
        # Сохраняем в базу
        if movie_id:
            save_opinion(
                movie_id=movie_id,
                short_opinion=short_opinion,
                full_opinion=full_response,
                mood_tags=mood_tags,
                atmosphere_tags=atmosphere_tags
            )
            user.record_user_opinion(user_obj.id, movie_id)
            
            # Увеличиваем соответствующий счетчик
            if force_regenerate:
                user.increment_stat_counter(user_obj.id, 'regeneration_count')
            else:
                user.increment_stat_counter(user_obj.id, 'opinion_count')
        
        # Формируем сообщение
        if is_callback:
            await update.callback_query.message.chat.send_action(action="typing")
        else:
            await context.bot.send_chat_action(chat_id=chat_id, action="typing")
        
        kp_url = f"https://www.kinopoisk.ru/film/{movie_id}/"
        title_with_link = f"<a href='{kp_url}'>{title}</a> ({year})"
        
        # Добавляем кнопки
        keyboard = [
            [InlineKeyboardButton("Получить свежий взгляд", callback_data=f"regenerate:{movie_id}:{year}")],
            [InlineKeyboardButton("Сообщить о неточности", callback_data=f"report_error:{movie_id}")],
            [InlineKeyboardButton("На корм и развитие навыков", callback_data="donate")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Формируем сообщение - всегда одинаковое, без благодарностей
        final_text = f"Я посмотрела {title_with_link}, и вот что думаю:\n\n{full_response}\n\n🐾"

        await context.bot.send_message(
            chat_id=chat_id,
            text=final_text,
            parse_mode='HTML',
            reply_markup=reply_markup
        )

    except Exception as e:
        logger.error(f"Ошибка AI: {e}")
        await context.bot.send_message(
            chat_id=chat_id,
            text="Гав! Кажется, я перегрызла провод... Попробуйте позже!"
        )


# ==================== ОБРАБОТКА КНОПОК ====================

async def handle_button_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    
    try:
        if data.startswith("ai:"):
            parts = data.split(":")
            if len(parts) == 3:
                _, movie_id, year = parts
                await handle_ai_message(update, context, movie_id=movie_id, year=year)
            else:
                await query.message.reply_text("Гав! Что-то не так с данными фильма...")
        
        elif data.startswith("regenerate:"):
            parts = data.split(":")
            if len(parts) == 3:
                _, movie_id, year = parts
                
                # Проверяем, админ ли пользователь
                if user.is_admin(query.from_user.id):
                    # Админ - запрашиваем причину
                    context.user_data['regenerate_movie_id'] = movie_id
                    context.user_data['regenerate_year'] = year
                    context.user_data['feedback_stage'] = 'awaiting_regenerate_reason'
                    
                    await query.message.reply_text(
                        "🐾 Расскажи, что именно не так с моим мнением? \n\n"
                        "Напиши несколько слов, и я сформирую свежий взгляд с учетом твоего замечания!"
                    )
                else:
                    # Обычный пользователь - просто генерируем новое мнение без пояснений
                    await handle_ai_message(
                        update, 
                        context, 
                        movie_id=movie_id, 
                        year=year, 
                        force_regenerate=True,
                        regeneration_reason=None
                    )
        
        elif data == "search_cancel":
            await query.edit_message_text("🐾 Поиск отменен. Если что - я всегда рядом!")
        
        elif data == "continue_search":
            is_premiers = context.user_data.get('is_premiers', False)
            await show_movies(update, context, is_premiers=is_premiers)
            
        elif data == "stop_search":
            await query.edit_message_text("🐾 Поиски завершены. Если что - я всегда рядом!\n\nЕсли ты не нашел фильм, то возможно его рейтинг не набрал еще 5 баллов и его нет среди новинок.\n\nЕсли это не так - сообщи мне через /feedback")
            context.user_data.clear()
            
        # Обработка FAQ
        elif data.startswith("faq_"):
            await handle_faq_button(update, context)
            
        # Обработка обратной связи
        elif data.startswith("feedback_"):
            await handle_feedback(update, context)
            
        elif data.startswith("admin_"):
            await handle_admin_callback(update, context)
        
        elif data.startswith("report_error:"):
            parts = data.split(":")
            if len(parts) == 2:
                _, movie_id = parts
                context.user_data['feedback_stage'] = 'awaiting_error_desc'
                context.user_data['feedback_type'] = 1
                context.user_data['movie_id'] = movie_id
                context.user_data['user_id'] = query.from_user.id
                
                await query.message.reply_text(
                    "🐾 Пожалуйста, опиши подробнее, что не так с моим мнением об этом фильме:",
                    parse_mode='HTML'
                )
            else:
                await query.message.reply_text("Гав! Что-то не так с данными фильма...")
        
        else:
            return False
            
    except Exception as e:
        logger.error(f"Ошибка обработки кнопки: {e}")
        await query.message.reply_text("Гав! Кажется, я перепутала следы... Попробуйте еще раз!")
    
    return True

# ==================== ОБРАБОТЧИКИ ДОНАТОВ ====================

async def handle_donate_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    payments_logger.info(f"User {query.from_user.id} initiated donation")
    
    oferta_url = config['Website']['oferta_donate_url']
    
    keyboard = [
        [InlineKeyboardButton("50 руб", callback_data="donate_50")],
        [InlineKeyboardButton("100 руб", callback_data="donate_100")],
        [InlineKeyboardButton("150 руб", callback_data="donate_150")],
        [
            InlineKeyboardButton("Оферта", url=oferta_url),
            InlineKeyboardButton("Закрыть", callback_data="donate_close")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await context.bot.send_message(
        chat_id=query.message.chat_id,
        text="🐾 Спасибо, что хотите поддержать мое развитие!\n"
             "Выберите сумму пожертвования:",
        reply_markup=reply_markup
    )

async def handle_donate_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data == "donate_close":
        await query.edit_message_text("🐾 Поняла, сворачиваю меню доната!")
        return
    
    if data == "skip_email":
        context.user_data['donate_email'] = None
        await ask_for_phone(update, context)
        return
    
    if data.startswith("donate_"):
        amount = int(data.split("_")[1])
        await ask_for_email(update, context, amount)


async def check_payment_status(payment_id: str):
    """Проверка статуса платежа согласно документации Tinkoff"""
    try:
        params = {
            "TerminalKey": config['Tinkoff']['terminal_key'],
            "PaymentId": payment_id,
            "Token": generate_token('GetState', {"PaymentId": payment_id})
        }
        
        logger.info(f"Отправка запроса статуса платежа: {params}")
        
        response = requests.post(
            config['Tinkoff']['state_url'],
            json=params,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        
        logger.info(f"Ответ от Tinkoff API: {data}")
        
        if not data.get('Success', False):
            logger.error(f"Ошибка в ответе Tinkoff: {data.get('Message', 'Неизвестная ошибка')}")
            return None
        
        return data
        
    except Exception as e:
        logger.error(f"Ошибка проверки статуса платежа: {str(e)}")
        return None


async def handle_check_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    payment_id = query.data.split("_")[2]
    
    conn = sqlite3.connect(config['Data']['payments_db_path'])
    cursor = conn.cursor()
    cursor.execute('''
        SELECT amount, status, created_at, payment_url, user_email, user_phone 
        FROM payments 
        WHERE payment_id = ?
    ''', (payment_id,))
    payment_data = cursor.fetchone()
    conn.close()
    
    if not payment_data:
        await query.edit_message_text("❌ Платеж не найден в базе данных")
        return
    
    amount, status, created_at, payment_url, user_email, user_phone = payment_data
    
    status_data = await check_payment_status(payment_id)
    if status_data and status_data.get('Success'):
        new_status = status_data['Status'].upper()
        if new_status != status:
            current_time = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
            conn = sqlite3.connect(config['Data']['payments_db_path'])
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE payments 
                SET status = ?, updated_at = ?
                WHERE payment_id = ?
            ''', (new_status, current_time, payment_id))
            conn.commit()
            conn.close()
            status = new_status
    
    if created_at:
        try:
            if isinstance(created_at, str):
                dt = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
            else:
                dt = created_at
            formatted_date = dt.strftime("%d.%m.%Y %H:%M")
        except Exception as e:
            logger.error(f"Ошибка форматирования даты: {e}")
            formatted_date = "неизвестно"
    else:
        formatted_date = "неизвестно"
    
    email_display = user_email if user_email else "не указан"
    phone_display = user_phone if user_phone else "не указан"
    
    status_text = {
        'NEW': '🆕 Ожидает оплаты (платеж создан)',
        'FORM_SHOWED': '📲 Открыта платежная форма',
        'AUTHORIZING': '🔄 Идет авторизация платежа',
        'AUTHORIZED': '⏳ Деньги заблокированы (ожидание подтверждения)',
        'CONFIRMING': '🔄 Подтверждение платежа',
        'CONFIRMED': '✅ Успешно оплачен',
        'REFUNDING': '↩️ Идет возврат средств',
        'ASYNC_REFUNDING': '↩️ Обработка возврата по QR',
        'PARTIAL_REFUNDED': '↩️ Частично возвращен',
        'REFUNDED': '↩️ Полностью возвращен',
        'CANCELED': '❌ Отменен мерчантом',
        'DEADLINE_EXPIRED': '⌛ Просрочен (истек срок оплаты)',
        'ATTEMPTS_EXPIRED': '❌ Превышены попытки оплаты',
        'REJECTED': '❌ Отклонен банком',
        'AUTH_FAIL': '❌ Ошибка авторизации/3D-Secure',
    }.get(status, f'❓ Неизвестный статус ({status})')
    
    keyboard = []
    
    if status in ['NEW', 'FORM_SHOWED', 'AUTHORIZING', 'AUTHORIZED'] and payment_url:
        keyboard.append([InlineKeyboardButton("Перейти к оплате", url=payment_url)])
    
    if status not in ['REJECTED', 'REFUNDED', 'CANCELED']:
        keyboard.append([InlineKeyboardButton("Обновить статус", callback_data=f"check_payment_{payment_id}")])
    
    keyboard.append([InlineKeyboardButton("Закрыть", callback_data="payment_close")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    message_text = (
        f"💰 <b>Статус платежа:</b> {status_text}\n"
        f"💳 <b>Сумма:</b> {amount:.2f} руб\n"
        f"📧 <b>Email:</b> {email_display}\n"
        f"📱 <b>Телефон:</b> {phone_display}\n"
        f"🆔 <b>ID платежа:</b> {payment_id}\n"
        f"📅 <b>Дата:</b> {formatted_date}"
    )
    
    await query.edit_message_text(
        text=message_text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )
    
    if status == 'CONFIRMED':
        donate_image_url = config['Images']['donate_image_url']
        caption = (
            "🐾 Гав-гав! Платеж успешно подтвержден!\n\n"
            "Спасибо за поддержку! Теперь я смогу стать еще умнее и находить для тебя самые лучшие фильмы! 🍿"
        )
        
        try:
            await context.bot.send_photo(
                chat_id=query.message.chat_id,
                photo=donate_image_url,
                caption=caption,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке изображения: {e}")
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="🐾 Спасибо за поддержку! Платеж успешно подтвержден!"
            )


# ==================== ОБРАБОТЧИКИ FAQ ====================

async def handle_faq_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await show_faq_menu(update, context)


async def handle_faq_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    context.user_data.pop('feedback_stage', None)
    context.user_data.pop('feedback_type', None)
    context.user_data.pop('movie_id', None)

    faq_data = {
        "faq_search": (
            "🔍 <b>Как найти фильм?</b>\n\n"
            "1. Используй команду /search для поиска по названию.\n"
            "2. Или /person для поиска фильма по актерам/режиссерам.\n"
            "3. А если хочешь свежайшие премьеры, то жми /premiers.\n"
            "4. Также можно попробовать /random для случайного фильма!\n\n"
            "🐾"
        ),
        "faq_opinion": (
            "💬 <b>Мнение КиноИщейки</b>\n\n"
            "Нажми кнопку «Мнение КиноИщейки» под карточкой фильма. "
            "Я расскажу, что думаю о фильме, его атмосфере и настроении!\n\n"
            "🐾"
        ),
        "faq_limits": (
            "⚠️ <b>Лимиты бота</b>\n\n"
            "У меня есть суточные лимиты на запросы мнений, чтобы я не переутомилась:\n"
            "- Тариф <b>Щенячий азарт</b>: 3 мнения в сутки (для всех)\n"
            "- Тариф <b>Ленивый хвост</b>: ** мнений в сутки (в разработке)\n"
            "- Тариф <b>Бдительный нюх</b>: ** мнений в сутки (в разработке)\n"
            "- Тариф <b>Неутомимый следопыт</b>: ** мнений в сутки (в разработке)\n\n"
            "Лимиты сбрасываются в полночь!\n\n"
            "🐾"
        ),
        "faq_donate": (
            "💰 <b>Как оплатить донат?</b>\n\n"
            "1. Нажми кнопку «На корм и развитие навыков» в пункте меню /start или /about\n"
            "2. Выбери сумму доната\n"
            "3. Укажи email (по желанию, помогает банку дополнительно верифицировать платеж)\n"
            "4. Введи телефон в формате +79000000000 (обязательно, является требованием банка для защиты от мошенников, мы не делаем рассылки и не передаем данные третьим лицам)\n"
            "5. Перейди по ссылке для оплаты\n"
            "6. После оплаты можешь проверить статус платежа\n\n"
            "📜 <a href='https://moviedog.tb.ru/oferta_donate'>Оферта о донатах</a>\n\n"
            "🐾 Спасибо за поддержку! Каждая монетка помогает мне становиться лучше!"
        ),
        "faq_suggest": (
            "📢 <b>Предложить улучшение</b>\n\n"
            "Если у тебя есть идеи, как сделать меня лучше, воспользуйся командой /feedback "
            "Я люблю апдейты и новые тренировки, как косточки! 🦴\n\n"
            "🐾"
        ),
        "faq_close": (
            "🐾 Поняла, сворачиваю меню FAQ!\n\n "
            "Если что-то понадобится - просто выбери в меню или введи /faq снова!"
        ),
        "faq_back": (
            "🐾 Возвращаемся в меню FAQ!"
        ),
    }
    
    if query.data == "faq_close":
        await query.edit_message_text(faq_data["faq_close"])
        return
    
    if query.data == "faq_back":
        await show_faq_menu(update, context)
        return
    
    answer = faq_data.get(query.data, "Гав! Я не нашла ответа на этот вопрос... Попробуй другой!")
    
    keyboard = [
        [InlineKeyboardButton("Вернуться в FAQ", callback_data="faq_back")],
        [InlineKeyboardButton("Закрыть", callback_data="faq_close")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        answer,
        parse_mode='HTML',
        reply_markup=reply_markup
    )


# ==================== ОБРАТНАЯ СВЯЗЬ ====================

def save_feedback(user_id, feedback_type, movie_id, message):
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO feedback (user_id, type, movie_id, message, status, created_at)
    VALUES (?, ?, ?, ?, 'new', ?)
    ''', (
        user_id,
        feedback_type,
        movie_id if movie_id else None,
        message,
        datetime.now().isoformat()
    ))
    conn.commit()
    conn.close()


def get_user_feedback(user_id):
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
    SELECT id, type, movie_id, message, status, admin_comment 
    FROM feedback 
    WHERE user_id = ? AND status != 'archive'
    ORDER BY created_at DESC
    ''', (user_id,))
    result = cursor.fetchall()
    conn.close()
    return result


async def feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("Сообщить об ошибке", callback_data="feedback_error")],
        [InlineKeyboardButton("Оставить отзыв", callback_data="feedback_review")],
        [InlineKeyboardButton("Мои обращения", callback_data="feedback_list")],
        [InlineKeyboardButton("Закрыть", callback_data="feedback_close")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = (
        "🐾 <b>Обратная связь</b>\n\n"
        "Здесь ты можешь:\n"
        "• Сообщить об ошибке\n"
        "• Оставить отзыв о моей работе\n"
        "• Посмотреть свои предыдущие обращения"
    )
    
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='HTML', reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, parse_mode='HTML', reply_markup=reply_markup)


async def handle_feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data in ["feedback_prev", "feedback_next"]:
        await handle_feedback_pagination(update, context)
        return
    
    context.user_data.pop('feedback_stage', None)
    context.user_data.pop('movie_id', None)
    
    if query.data == "feedback_close":
        await query.edit_message_text("🐾 Поняла, сворачиваю меню обратной связи!")
        return
    
    if query.data == "feedback_list":
        await show_user_feedback(update, context)
        return
    
    if query.data == "feedback_back":
        await feedback(update, context)
        return
    
    context.user_data['feedback_type'] = 1 if query.data == "feedback_error" else 2
    
    if query.data == "feedback_error":
        text = (
            "🐾 <b>Помоги мне исправить ошибку!</b>\n\n"
            "Для быстрого решения укажи ID фильма одним из способов:\n\n"
            "🔹 <b>Способ 1</b> - В карточке фильма в боте:\n"
            "1. Найди сообщение с фильмом\n"
            "2. Посмотри в первой строке - там есть ссылка\n"
            "   Например: <code>https://www.kinopoisk.ru/film/23200/</code>\n"
            "3. Цифры в конце ссылки - это ID (в примере: 23200)\n\n"
            "🔹 <b>Способ 2</b> - На сайте Кинопоиска:\n"
            "1. Открой карточку фильма в браузере\n"
            "2. ID будет в адресной строке\n"
            "   Например: <code>https://www.kinopoisk.ru/film/435/</code>\n\n"
            "🔹 <b>Популярные примеры:</b>\n"
            "• 'Зеленая миля' → 435\n"
            "• 'Назад в будущее' → 476\n"
            "• 'Друзья' → 77044\n\n"
            "📌 ID всегда число от 1 до 10 цифр\n\n"
            "Если ошибка не связана с фильмом или не нашел ID,\n"
            "просто напиши <b>'нет'</b> и в следующем сообщении опиши проблему:"
        )
        context.user_data['feedback_stage'] = 'awaiting_movie_id'
    else:
        text = "🐾 Напиши свой отзыв о моих навыках:"
        context.user_data['feedback_stage'] = 'awaiting_review'
    
    await query.edit_message_text(text, parse_mode='HTML')


async def process_feedback_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Проверяем, ожидаем ли мы email для пожертвования
    if context.user_data.get('donate_stage') == 'awaiting_email':
        text = update.message.text.strip()
        if re.match(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', text):
            context.user_data['donate_email'] = text
            await ask_for_phone(update, context)
        elif text.lower() == 'пропустить':
            context.user_data['donate_email'] = None
            await ask_for_phone(update, context)
        else:
            keyboard = [
                [InlineKeyboardButton("Пропустить", callback_data="skip_email")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                "🐾 Это не похоже на email. Пожалуйста, введи корректный email или нажми 'Пропустить':",
                reply_markup=reply_markup
            )
        return
    
    # Обработка телефона для пожертвования
    if context.user_data.get('donate_stage') == 'awaiting_phone':
        text = update.message.text.strip()
        
        if re.match(r'^\+\d{11}$', text):
            context.user_data['donate_phone'] = text
            
            await process_donation(
                update, 
                context, 
                context.user_data['donate_amount'],
                context.user_data.get('donate_email'),
                context.user_data['donate_phone']
            )
            
            context.user_data.pop('donate_stage', None)
            context.user_data.pop('donate_amount', None)
            context.user_data.pop('donate_email', None)
            context.user_data.pop('donate_phone', None)
        else:
            await update.message.reply_text(
                "❌ Неверный формат телефона. Пожалуйста, введи телефон в формате +79000000000:"
            )
        return

    # Обработка поиска пользователя из админки
    if context.user_data.get('admin_mode') == 'searching_user':
        query_text = update.message.text.strip()
        user_obj = update.message.from_user
        
        if not admin.is_admin(user_obj.id):
            context.user_data.pop('admin_mode', None)
            await update.message.reply_text("🐾 Эта команда только для моих тренеров!")
            return
        
        # Выполняем поиск
        users = admin.search_users(query_text)
        
        if not users:
            keyboard = [[InlineKeyboardButton("◀️ В админку", callback_data="admin_back")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                "❌ Пользователи не найдены. Попробуй другой запрос.",
                reply_markup=reply_markup
            )
            context.user_data.pop('admin_mode', None)
            return
        
        # Показываем результаты поиска
        text = f"🔍 <b>Результаты поиска \"{query_text}\":</b>\n\n"
        
        for u in users[:5]:  # Показываем первые 5 результатов
            user_id, username, first_name, last_name, reg_date, today_opinions, today_regens = u
            name = first_name or username or "Без имени"
            reg_date_str = reg_date[:10] if reg_date else "неизвестно"
            username_display = f"@{username}" if username else "нет username"
            
            text += f"• <b>{name}</b> (ID: {user_id})\n"
            text += f"  {username_display}, рег: {reg_date_str}\n"
            text += f"  📊 сегодня: {today_opinions} мнений\n\n"
        
        # Добавляем кнопки для каждого найденного пользователя
        keyboard = []
        for u in users[:5]:
            user_id = u[0]
            name = u[2] or u[1] or f"ID {user_id}"
            display_name = name[:15] + ("..." if len(name) > 15 else "")
            keyboard.append([InlineKeyboardButton(
                f"👤 {display_name}", 
                callback_data=f"admin_user_details_{user_id}"
            )])
        
        # Добавляем кнопки навигации
        keyboard.append([
            InlineKeyboardButton("◀️ Назад", callback_data="admin_users_search"),
            InlineKeyboardButton("🚪 В админку", callback_data="admin_back")
        ])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(text, parse_mode='HTML', reply_markup=reply_markup)
        context.user_data.pop('admin_mode', None)
        return

    # Обработка поиска фильмов из админки
    if context.user_data.get('admin_mode') == 'searching_movie':
        query_text = update.message.text.strip()
        user_obj = update.message.from_user
        
        if not admin.is_admin(user_obj.id):
            context.user_data.pop('admin_mode', None)
            await update.message.reply_text("🐾 Эта команда только для моих тренеров!")
            return
        
        # Выполняем поиск фильмов
        movies = admin.search_movies_admin(query_text, limit=10)
        
        if not movies:
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_movies")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                "❌ Фильмы не найдены. Попробуй другой запрос.",
                reply_markup=reply_markup
            )
            context.user_data.pop('admin_mode', None)
            return
        
        text = f"🔍 <b>Результаты поиска \"{query_text}\":</b>\n\n"
        
        for movie in movies:
            id, name, year, rating, movie_type, is_new, await_count = movie
            type_icon = {
                'movie': '🎬',
                'tv-series': '📺',
                'cartoon': '🎨',
                'animated-series': '🎨'
            }.get(movie_type, '🎬')
            
            new_badge = " 🆕" if is_new else ""
            rating_display = f"★ {rating:.1f}" if rating else "нет рейтинга"
            
            text += f"{type_icon} <b>{name}</b> ({year}){new_badge}\n"
            text += f"   ID: <code>{id}</code> | {rating_display}\n"
            text += f"   👥 {await_count} ожиданий\n\n"
        
        # Кнопки для каждого фильма
        keyboard = []
        for movie in movies[:5]:
            id, name, year, _, _, _, _ = movie
            display_name = name[:20] + ("..." if len(name) > 20 else "")
            keyboard.append([InlineKeyboardButton(
                f"🎬 {display_name} ({year})",
                callback_data=f"admin_movie_details_{id}"
            )])
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_movies")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(text, parse_mode='HTML', reply_markup=reply_markup)
        context.user_data.pop('admin_mode', None)
        return

    # Обработка причины для свежего взгляда
    if context.user_data.get('feedback_stage') == 'awaiting_regenerate_reason':
        if not user.is_admin(update.message.from_user.id):
            await update.message.reply_text("🐾 Извини, эта функция только для администраторов.")
            context.user_data.clear()
            return
        
        reason = update.message.text.strip()
        movie_id = context.user_data.get('regenerate_movie_id')
        year_val = context.user_data.get('regenerate_year')
        
        if not movie_id or not year_val:
            await update.message.reply_text("Гав! Что-то пошло не так. Попробуй еще раз найти фильм.")
            context.user_data.clear()
            return
        
        await update.message.reply_text("🐕‍🦺 Поняла! Сейчас пересмотрю фильм с учетом твоего замечания...")
        
        class MockUser:
            def __init__(self, usr):
                self.id = usr.id
                self.username = usr.username
                self.first_name = usr.first_name
                self.last_name = usr.last_name
        
        mock_callback_query = type('MockCallbackQuery', (), {
            'from_user': MockUser(update.message.from_user),
            'message': update.message,
            'answer': lambda: None
        })()
        
        await handle_ai_message(
            update, 
            context, 
            movie_id=movie_id, 
            year=year_val, 
            force_regenerate=True,
            regeneration_reason=reason,
            mock_callback_query=mock_callback_query
        )
        
        context.user_data.pop('regenerate_movie_id', None)
        context.user_data.pop('regenerate_year', None)
        context.user_data.pop('feedback_stage', None)
        return  
    
    # Обработка текстового сообщения с обратной связью
    user_id = update.message.from_user.id
    text = update.message.text.strip() if update.message.text else ""
    MAX_LENGTH = 1000

    if not context.user_data.get('feedback_stage'):
        await handle_text(update, context)
        return

    if len(text) > MAX_LENGTH:
        warning_msg = (
            "⚠️ <b>Слишком длинное сообщение</b>\n\n"
            f"Я сократила твое сообщение до первых {MAX_LENGTH} символов.\n"
            "Если нужно отправить больше информации, раздели сообщение на несколько частей."
        )
        await update.message.reply_text(warning_msg, parse_mode='HTML')
        text = text[:MAX_LENGTH]

    if context.user_data['feedback_stage'] == 'awaiting_movie_id':
        if text.lower() == 'нет':
            context.user_data['movie_id'] = None
            await update.message.reply_text(
                "🐾 Теперь опиши подробнее что волнует:",
                parse_mode='HTML'
            )
            context.user_data['feedback_stage'] = 'awaiting_error_desc'
        elif text.isdigit() and 2 < len(text) <= 10 and int(text) != 0:
            context.user_data['movie_id'] = text
            await update.message.reply_text(
                "🐾 Теперь опиши что не так с этим фильмом:",
                parse_mode='HTML'
            )
            context.user_data['feedback_stage'] = 'awaiting_error_desc'
        else:
            await update.message.reply_text(
                "🐾 ID фильма должен быть числом от 3 до 10 цифр. Попробуй еще раз или введи 'нет':",
                parse_mode='HTML'
            )
            return

    elif context.user_data['feedback_stage'] == 'awaiting_error_desc':
        feedback_type = context.user_data.get('feedback_type', 1)
        movie_id_val = context.user_data.get('movie_id')
        
        save_feedback(
            user_id=user_id,
            feedback_type=feedback_type,
            movie_id=movie_id_val,
            message=text
        )
        
        keyboard = [
            [InlineKeyboardButton("Вернуться в меню", callback_data="feedback_back")],
            [InlineKeyboardButton("Закрыть", callback_data="feedback_close")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        response_text = (
            "🐾 Гав-гав! Спасибо за бдительность!\n\n"
            "Я записала твое сообщение и уже бегу разбираться. "
            "Мои тренеры проверят информацию и обязательно всё исправят!\n\n"
            "А пока можешь продолжить поиски отличного кино - мой нюх никогда не подводит! 🍿"
        )
        
        await update.message.reply_text(
            response_text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        context.user_data.pop('feedback_stage', None)
        context.user_data.pop('movie_id', None)
        context.user_data.pop('feedback_type', None)

    elif context.user_data['feedback_stage'] == 'awaiting_review':
        save_feedback(
            user_id=user_id,
            feedback_type=2,
            movie_id=None,
            message=text
        )
        
        keyboard = [
            [InlineKeyboardButton("Вернуться в меню", callback_data="feedback_back")],
            [InlineKeyboardButton("Закрыть", callback_data="feedback_close")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "🐾 Спасибо за отзыв! Очень ценно твое мнение.",
            reply_markup=reply_markup
        )
        
        context.user_data.pop('feedback_stage', None)


async def show_user_feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    feedback_list = get_user_feedback(query.from_user.id)
    if not feedback_list:
        await query.edit_message_text("🐾 У тебя пока нет обращений.")
        return
    
    context.user_data['feedback_list'] = feedback_list
    context.user_data['feedback_page'] = 0
    
    await show_feedback_page(update, context)


async def handle_feedback_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    current_page = context.user_data.get('feedback_page', 0)
    
    if query.data == "feedback_prev":
        context.user_data['feedback_page'] = max(0, current_page - 1)
    elif query.data == "feedback_next":
        feedback_list = context.user_data.get('feedback_list', [])
        items_per_page = 5
        total_pages = (len(feedback_list) + items_per_page - 1) // items_per_page
        context.user_data['feedback_page'] = min(total_pages - 1, current_page + 1)
    
    await show_feedback_page(update, context)


async def show_feedback_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    feedback_list = context.user_data.get('feedback_list', [])
    page = context.user_data.get('feedback_page', 0)
    items_per_page = 5
    total_pages = max(1, (len(feedback_list) + items_per_page - 1) // items_per_page)
    
    status_translation = {
        'new': '🆕 Новое',
        'in_progress': '🔄 В обработке',
        'resolved': '✅ Решено',
        'archive': '🗄 Архив'
    }
    
    text = f"🐾 <b>Твои обращения (стр. {page+1}/{total_pages}):</b>\n\n"
    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    
    for item in feedback_list[start_idx:end_idx]:
        fid, ftype, movie_id_val, message, status, comment = item
        translated_status = status_translation.get(status, status)
        
        text += (
            f"<b>#{fid}</b> {translated_status}\n"
            f"Тип: {'🛠 Ошибка' if ftype == 1 else '📢 Отзыв'}\n"
        )
        if movie_id_val:
            text += f"Фильм: <a href='https://www.kinopoisk.ru/film/{movie_id_val}/'>{movie_id_val}</a>\n"
        text += f"Сообщение: {message[:200]}...\n" if len(message) > 200 else f"Сообщение: {message}\n"
        if comment:
            text += f"<i>Комментарий:</i> {comment[:200]}...\n" if len(comment) > 200 else f"<i>Комментарий:</i> {comment}\n"
        text += "\n"
    
    keyboard = []
    
    if total_pages > 1:
        row = []
        if page > 0:
            row.append(InlineKeyboardButton("⬅️ Назад", callback_data="feedback_prev"))
        if page < total_pages - 1:
            row.append(InlineKeyboardButton("Вперёд ➡️", callback_data="feedback_next"))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("Назад в меню", callback_data="feedback_back")])
    keyboard.append([InlineKeyboardButton("Закрыть", callback_data="feedback_close")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            text, 
            parse_mode='HTML', 
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            text, 
            parse_mode='HTML', 
            reply_markup=reply_markup
        )


async def process_donation(update: Update, context: ContextTypes.DEFAULT_TYPE, amount: int, user_email: str = None, user_phone: str = None):
    user_obj = update.callback_query.from_user if update.callback_query else update.message.from_user
    
    if not user_email:
        user_email = "test@example.com"
    
    if not user_phone:
        user_phone = ""
    
    payment_data = await init_payment(
        user_id=user_obj.id,
        amount=amount,
        description=f"Пожертвование на развитие КиноИщейки ({amount} руб)",
        user_email=user_email,
        user_phone=user_phone
    )
    
    if payment_data and payment_data.get('Success'):
        conn = sqlite3.connect(config['Data']['payments_db_path'])
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE payments 
            SET payment_url = ?, user_email = ?, user_phone = ?
            WHERE payment_id = ?
        ''', (payment_data['PaymentURL'], user_email, user_phone, payment_data['PaymentId']))
        conn.commit()
        conn.close()
        
        keyboard = [
            [InlineKeyboardButton("Перейти к оплате", url=payment_data['PaymentURL'])],
            [InlineKeyboardButton("Проверить статус", callback_data=f"check_payment_{payment_data['PaymentId']}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await context.bot.send_message(
            chat_id=user_obj.id,
            text=f"🐾 Спасибо за поддержку! Ссылка для оплаты {amount} руб:",
            reply_markup=reply_markup
        )
    else:
        await context.bot.send_message(
            chat_id=user_obj.id,
            text="❌ Не удалось создать платеж. Пожалуйста, попробуйте позже."
        )


async def show_tariff_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_obj = update.message.from_user if update.message else update.callback_query.from_user
    limits = user.get_user_limits(user_obj.id)
    
    try:
        end_date = datetime.fromisoformat(limits['tariff_end_date'])
        formatted_date = end_date.strftime("%d.%m.%Y")
    except:
        formatted_date = "неизвестно"
    
    text = (
        f"🐾 <b>Твой текущий тариф:</b> {limits['tariff_name']}\n\n"
        f"📊 <b>Лимиты:</b>\n"
        f"- Мнений в сутки: {limits['opinion_limit']}\n"
        f"- Свежих взглядов в сутки: {limits['regeneration_limit']}\n\n"
        f"📅 <b>Действует до:</b> {formatted_date}"
    )
    
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=text,
        parse_mode='HTML'
    )

# ==================== ОБРАБОТКА КНОПОК АДМИНА ====================
async def handle_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка callback'ов из админки"""
    query = update.callback_query
    await query.answer()
    
    user_obj = query.from_user
    
    if not admin.is_admin(user_obj.id):
        await query.edit_message_text("🐾 Это команда только для моих тренеров!")
        return
    
    data = query.data
    
    if data == "admin_close":
        await query.edit_message_text("🐕‍🦺 Панель управления закрыта")
        return
    
    if data == "admin_users":
        # Показываем топ активных пользователей за 7 дней
        users = admin.get_top_active_users(limit=10, days=7)
        
        # Проверяем, есть ли вообще пользователи
        if not users:
            text = "👥 <b>Пользователи</b>\n\nВ базе пока нет ни одного пользователя 🥺"
            keyboard = [
                [InlineKeyboardButton("🏠 Главное меню", callback_data="admin_back")],
                [InlineKeyboardButton("❌ Закрыть", callback_data="admin_close")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, parse_mode='HTML', reply_markup=reply_markup)
            return
        
        # Проверяем, была ли активность за 7 дней
        has_activity = any(u[9] > 0 for u in users)  # total_actions
        
        if not has_activity:
            text = "👥 <b>Последние зарегистрированные пользователи</b>\n\n"
            text += "За последние 7 дней не было активности 🥺\n\n"
        else:
            # Считаем общую статистику только по активным
            active_users = [u for u in users if u[9] > 0]
            total_actions_all = sum(u[9] for u in active_users)
            total_active = len(active_users)
            avg_actions = total_actions_all // total_active if total_active > 0 else 0
            
            text = f"👥 <b>Топ-10 за 7 дней</b>\n\n"
            text += f"📊 Всего действий: {total_actions_all}\n"
            text += f"👤 Активных пользователей: {total_active}\n"
            text += f"📈 В среднем: {avg_actions} действий\n\n"
        
        for i, u in enumerate(users, 1):
            (user_id, username, first_name, last_name, reg_date, 
             total_opinions, total_regens, total_searches, total_kp,
             total_actions, last_active, active_days, total_opinions_alltime, days_since_reg) = u
            
            name = first_name or username or "Без имени"
            if len(name) > 25:
                name = name[:25] + "..."
            
            if total_actions > 0:
                medal = ""
                if i == 1 and has_activity:
                    medal = "🥇 "
                elif i == 2 and has_activity:
                    medal = "🥈 "
                elif i == 3 and has_activity:
                    medal = "🥉 "
                text += f"{medal}🔥 <b>{name}</b> (ID: {user_id})\n"
                text += f"   Действий: {total_actions} | Активен: {active_days}/7 дн\n\n"
            else:
                reg_date_str = reg_date[:10] if reg_date else "?"
                text += f"💤 <b>{name}</b> (ID: {user_id})\n"
                text += f"   Регистрация: {reg_date_str}\n\n"
        
        # Кнопки пользователей - все 10, по 2 в ряд
        keyboard = []
        user_buttons = []
        
        for u in users:
            user_id = u[0]
            name = u[2] or u[1] or f"ID {user_id}"
            display_name = name[:12] + ("..." if len(name) > 12 else "")
            
            if u[9] > 0:
                display_name = f"🔥{display_name}"
            else:
                display_name = f"💤{display_name}"
            
            user_buttons.append(InlineKeyboardButton(
                display_name, 
                callback_data=f"admin_user_details_{user_id}"
            ))
        
        for i in range(0, len(user_buttons), 2):
            if i + 1 < len(user_buttons):
                keyboard.append([user_buttons[i], user_buttons[i + 1]])
            else:
                keyboard.append([user_buttons[i]])
        
        keyboard.append([
            InlineKeyboardButton("🔍 Поиск", callback_data="admin_users_search")
        ])
        keyboard.append([
            InlineKeyboardButton("◀️ Назад", callback_data="admin_back"),
            InlineKeyboardButton("🏠 Главное", callback_data="admin_back"),
            InlineKeyboardButton("❌ Закрыть", callback_data="admin_close")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=reply_markup)
        return
    
    elif data == "admin_users_search":
        # Переводим бота в режим поиска пользователей
        context.user_data['admin_mode'] = 'searching_user'
        
        # Кнопки навигации
        keyboard = [
            [InlineKeyboardButton("◀️ Назад к топ-10", callback_data="admin_users")],
            [InlineKeyboardButton("❌ Закрыть", callback_data="admin_close")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "🔍 <b>Поиск пользователя</b>\n\n"
            "Введи ID пользователя, username или имя для поиска:\n\n"
            "• ID — точное совпадение\n"
            "• username — без @\n"
            "• имя — часть имени",
            parse_mode='HTML',
            reply_markup=reply_markup
        )
        return
    
    elif data.startswith("admin_user_details_"):
        user_id = int(data.replace("admin_user_details_", ""))
        stats = admin.get_user_full_stats(user_id)
        
        if not stats:
            await query.edit_message_text("❌ Пользователь не найден")
            return
        
        info = stats['info']
        user_id, username, first_name, last_name, reg_date, total_opinions, last_active = info
        limits = stats['limits']
        today_opinions, today_regens, today_searches, today_kp = stats['today_stats']
        weekly_stats = stats['weekly_stats']
        best_day = stats['best_day']
        feedback_total, feedback_new, feedback_resolved = stats['feedback_stats']
        recent_movies = stats['recent_movies']
        
        # Форматируем основную информацию
        name_parts = []
        if first_name:
            name_parts.append(first_name)
        if last_name:
            name_parts.append(last_name)
        full_name = " ".join(name_parts) if name_parts else "Не указано"
        
        username_display = f"@{username}" if username else "нет"
        reg_date_str = reg_date[:10] if reg_date else "неизвестно"
        last_active_str = last_active[:10] if last_active else "нет активности"
        
        # Определяем иконку тарифа
        tariff_icon = {
            'Щенячий азарт': '🐶',
            'Охотничий': '🐕',
            'Ищейка': '🕵️‍♀️',
            'Вожак': '🐺'
        }.get(limits['tariff_name'], '🐾')
        
        text = f"👤 <b>Карточка пользователя</b>\n\n"
        
        text += f"<b>ID:</b> <code>{user_id}</code>\n"
        text += f"<b>Имя:</b> {full_name}\n"
        text += f"<b>Username:</b> {username_display}\n"
        text += f"<b>Регистрация:</b> {reg_date_str}\n"
        text += f"<b>Последняя активность:</b> {last_active_str}\n\n"
        
        text += f"<b>{tariff_icon} Тариф: {limits['tariff_name']}</b>\n"
        text += f"📅 Действует до: {limits['tariff_end_date'][:10]}\n\n"
        
        text += f"<b>📊 Лимиты и использование сегодня:</b>\n"
        text += f"• Мнений: {today_opinions}/{limits['opinion_limit']}\n"
        text += f"• Свежих взглядов: {today_regens}/{limits['regeneration_limit']}\n"
        text += f"• Поисков: {today_searches}/{limits['custom_query_limit']}\n"
        text += f"• Запросов к КП: {today_kp}/{limits['kinopoisk_query_limit']}\n\n"
        
        text += f"<b>📈 Общая статистика:</b>\n"
        text += f"• Всего мнений: {total_opinions}\n"
        
        if best_day and best_day[1] > 0:
            text += f"• Самый активный день: {best_day[0]} ({best_day[1]} действий)\n"
        
        text += f"\n"
        
        # Статистика за неделю
        if weekly_stats:
            text += f"<b>📅 Активность за 7 дней:</b>\n"
            for day in weekly_stats:
                date_str, opinions, regens, searches, kp = day
                total = opinions + regens + searches + kp
                if total > 0:
                    text += f"• {date_str}: {total} действий (🎬 {opinions} | 🔄 {regens})\n"
            text += f"\n"
        
        # Обращения
        if feedback_total > 0:
            text += f"<b>📝 Обращения:</b>\n"
            text += f"• Всего: {feedback_total}\n"
            text += f"• Новых: {feedback_new}\n"
            text += f"• Решённых: {feedback_resolved}\n\n"
        
        # Последние фильмы
        if recent_movies:
            text += f"<b>🎬 Последние просмотренные фильмы:</b>\n"
            for movie_id, movie_name, viewed_at in recent_movies:
                viewed_date = viewed_at[:10] if viewed_at else "?"
                text += f"• <a href='https://www.kinopoisk.ru/film/{movie_id}/'>{movie_name}</a> ({viewed_date})\n"
        
        # Кнопки управления
        keyboard = [
            [InlineKeyboardButton("✏️ Редактировать лимиты", callback_data=f"admin_edit_limits_{user_id}")],
            [InlineKeyboardButton("📝 Показать обращения", callback_data=f"admin_user_feedback_{user_id}")],
            [InlineKeyboardButton("📊 Полная статистика", callback_data=f"admin_user_stats_{user_id}")],
            [InlineKeyboardButton("◀️ Назад к списку", callback_data="admin_users")],
            [InlineKeyboardButton("🏠 В главное меню", callback_data="admin_back")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=reply_markup, disable_web_page_preview=True)
        return
    
    elif data == "admin_back":
        # Возврат в главное меню админки
        keyboard = [
            [InlineKeyboardButton("👥 Пользователи", callback_data="admin_users")],
            [InlineKeyboardButton("🎬 Фильмы", callback_data="admin_movies")],
            [InlineKeyboardButton("💭 Мнения", callback_data="admin_opinions")],
            [InlineKeyboardButton("📝 Обращения", callback_data="admin_feedback")],
            [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton("❌ Закрыть", callback_data="admin_close")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "🐕‍🦺 <b>Панель управления КиноИщейки</b>\n\nВыбери раздел:",
            parse_mode='HTML',
            reply_markup=reply_markup
        )
        return
    
    elif data == "admin_movies":
        # Меню управления фильмами
        text = "🎬 <b>Управление фильмами</b>\n\nВыбери действие:"
        
        keyboard = [
            [InlineKeyboardButton("🔍 Поиск фильма", callback_data="admin_movies_search")],
            [InlineKeyboardButton("🎉 Юбилейные подборки", callback_data="admin_movies_anniversary")],
            [InlineKeyboardButton("⭐ Ожидаемые новинки", callback_data="admin_movies_upcoming")],
            [InlineKeyboardButton("💭 Просмотр мнений", callback_data="admin_movies_opinions")],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_back")],
            [InlineKeyboardButton("🏠 Главное", callback_data="admin_back")],
            [InlineKeyboardButton("❌ Закрыть", callback_data="admin_close")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=reply_markup)
        return
    
    elif data == "admin_movies_search":
        # Режим поиска фильмов
        context.user_data['admin_mode'] = 'searching_movie'
        keyboard = [
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_movies")],
            [InlineKeyboardButton("❌ Закрыть", callback_data="admin_close")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "🔍 <b>Поиск фильма</b>\n\n"
            "Введи название фильма для поиска:",
            parse_mode='HTML',
            reply_markup=reply_markup
        )
        return
    
    elif data == "admin_movies_anniversary":
        # Показываем выбор месяца
        from datetime import datetime
        
        # Названия месяцев
        month_names = {
            1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
            5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
            9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
        }
        
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        text = f"📅 <b>Выбери месяц для юбилейных подборок</b>\n\n"
        text += f"Сейчас показываем: {month_names[current_month]} {current_year}\n\n"
        text += "Фильмы должны соответствовать:\n"
        text += "• Рейтинг ≥7.5\n"
        text += "• Премьера в выбранном месяце\n"
        text += "• С момента премьеры ≥20 лет\n"
        text += "• Возраст кратен 5 (юбилей)\n"
        text += "• Топ-100 по рейтингу"
        
        # Кнопки для выбора месяца
        keyboard = []
        
        # Первая строка: Январь-Март
        keyboard.append([
            InlineKeyboardButton("Янв", callback_data="anniversary_month_1"),
            InlineKeyboardButton("Фев", callback_data="anniversary_month_2"),
            InlineKeyboardButton("Мар", callback_data="anniversary_month_3")
        ])
        
        # Вторая строка: Апрель-Июнь
        keyboard.append([
            InlineKeyboardButton("Апр", callback_data="anniversary_month_4"),
            InlineKeyboardButton("Май", callback_data="anniversary_month_5"),
            InlineKeyboardButton("Июн", callback_data="anniversary_month_6")
        ])
        
        # Третья строка: Июль-Сентябрь
        keyboard.append([
            InlineKeyboardButton("Июл", callback_data="anniversary_month_7"),
            InlineKeyboardButton("Авг", callback_data="anniversary_month_8"),
            InlineKeyboardButton("Сен", callback_data="anniversary_month_9")
        ])
        
        # Четвертая строка: Октябрь-Декабрь
        keyboard.append([
            InlineKeyboardButton("Окт", callback_data="anniversary_month_10"),
            InlineKeyboardButton("Ноя", callback_data="anniversary_month_11"),
            InlineKeyboardButton("Дек", callback_data="anniversary_month_12")
        ])
        
        # Навигация
        keyboard.append([
            InlineKeyboardButton("◀️ Назад", callback_data="admin_movies")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=reply_markup)
        return
    
    elif data.startswith("anniversary_month_"):
        month = int(data.replace("anniversary_month_", ""))
        from datetime import datetime
        
        current_year = datetime.now().year
        
        month_names = {
            1: "январь", 2: "февраль", 3: "март", 4: "апрель",
            5: "май", 6: "июнь", 7: "июль", 8: "август",
            9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь"
        }
        
        await query.edit_message_text(f"🔍 Загружаю юбилейные фильмы за {month_names[month]} {current_year}...")
        
        movies = admin.get_anniversary_movies(
            year=current_year, 
            month=month, 
            min_rating=7.5,
            limit=100
        )
        
        if not movies:
            text = f"🎉 <b>Юбилейные фильмы за {month_names[month]} {current_year}</b>\n\n"
            text += "Фильмы не найдены 🥺"
            keyboard = [
                [InlineKeyboardButton("📅 Выбрать другой месяц", callback_data="admin_movies_anniversary")],
                [InlineKeyboardButton("◀️ Назад", callback_data="admin_movies")]
            ]
        else:
            # Сохраняем фильмы в контекст для возможного экспорта
            context.user_data['last_anniversary_movies'] = movies
            context.user_data['last_anniversary_month'] = month
            context.user_data['last_anniversary_year'] = current_year
            
            text = f"🎉 <b>Юбилейные фильмы за {month_names[month]} {current_year}</b>\n\n"
            text += f"Всего найдено: {len(movies)} фильмов\n"
            text += f"Рейтинг ≥7.5, юбилей ≥20 лет (кратно 5)\n\n"
            
            # Группируем по возрасту
            by_age = {}
            for movie in movies:
                age = movie['years_since']
                if age not in by_age:
                    by_age[age] = []
                by_age[age].append(movie)
            
            ages = sorted(by_age.keys(), reverse=True)
            max_ages_to_show = 5
            ages_to_show = ages[:max_ages_to_show]
            
            if len(ages) > max_ages_to_show:
                text += f"<i>Показаны {max_ages_to_show} самых старых юбилеев из {len(ages)}</i>\n\n"
            
            for age in ages_to_show:
                movies_to_show = by_age[age][:5]
                text += f"\n<b>{age} лет ({len(by_age[age])} фильмов):</b>\n"
                for movie in movies_to_show:
                    rating_display = f"★ {movie['rating']:.1f}" if movie['rating'] else "нет рейтинга"
                    text += f"• <a href='{movie['kp_url']}'><b>{movie['name']}</b></a> ({movie['release_year']}) — {rating_display}\n"
                    text += f"  Премьера: {movie['premiere_date']}\n"
            
            total_shown = sum(len(by_age[age][:5]) for age in ages_to_show)
            if total_shown < len(movies):
                text += f"\n<i>... и ещё {len(movies) - total_shown} фильмов</i>"
            
            # Кнопки с экспортом
            keyboard = [
                [InlineKeyboardButton("📥 Скачать полный список (CSV)", callback_data="export_anniversary_csv")],
                [InlineKeyboardButton("📅 Выбрать другой месяц", callback_data="admin_movies_anniversary")],
                [InlineKeyboardButton("◀️ Назад", callback_data="admin_movies")]
            ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            text, 
            parse_mode='HTML', 
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
        return
    
    elif data == "export_anniversary_csv":
        # Проверяем, есть ли данные для экспорта
        movies = context.user_data.get('last_anniversary_movies')
        month = context.user_data.get('last_anniversary_month')
        year = context.user_data.get('last_anniversary_year')
        
        if not movies or not month or not year:
            await query.edit_message_text(
                "❌ Нет данных для экспорта. Сначала выбери месяц.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Назад", callback_data="admin_movies_anniversary")
                ]])
            )
            return
        
        await query.edit_message_text("📥 Генерирую файл...")
        
        # Генерируем CSV
        file_path = admin.generate_anniversary_csv(movies, year, month)
        
        month_names = {
            1: "январь", 2: "февраль", 3: "март", 4: "апрель",
            5: "май", 6: "июнь", 7: "июль", 8: "август",
            9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь"
        }
        
        # Отправляем файл
        filename = f"юбилеи_{month_names[month]}_{year}.csv"
        with open(file_path, 'rb') as f:
            await context.bot.send_document(
                chat_id=query.message.chat_id,
                document=f,
                filename=filename,
                caption=f"🎉 Юбилейные фильмы за {month_names[month]} {year}\nВсего: {len(movies)} фильмов"
            )
        
        # Чистим временный файл
        import os
        os.unlink(file_path)
        
        # Возвращаемся к списку
        keyboard = [
            [InlineKeyboardButton("📅 Выбрать другой месяц", callback_data="admin_movies_anniversary")],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_movies")]
        ]
        await query.edit_message_text(
            "✅ Файл отправлен!",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
            
    elif data == "admin_movies_anniversary_select":
        # TODO: добавить выбор месяца
        text = "📅 <b>Выбор месяца</b>\n\nЭта функция будет добавлена позже.\n\nПока показываем за текущий месяц."
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_movies_anniversary")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=reply_markup)
        return
    
    elif data == "admin_movies_upcoming":
        # Показываем выбор месяца для новинок
        text = "📅 <b>Выбери месяц для ожидаемых новинок</b>\n\n"
        text += "Покажу топ-10 самых ожидаемых фильмов с премьерой в выбранном месяце."
        
        # Кнопки для выбора месяца
        keyboard = []
        
        keyboard.append([
            InlineKeyboardButton("Янв", callback_data="upcoming_month_1"),
            InlineKeyboardButton("Фев", callback_data="upcoming_month_2"),
            InlineKeyboardButton("Мар", callback_data="upcoming_month_3")
        ])
        keyboard.append([
            InlineKeyboardButton("Апр", callback_data="upcoming_month_4"),
            InlineKeyboardButton("Май", callback_data="upcoming_month_5"),
            InlineKeyboardButton("Июн", callback_data="upcoming_month_6")
        ])
        keyboard.append([
            InlineKeyboardButton("Июл", callback_data="upcoming_month_7"),
            InlineKeyboardButton("Авг", callback_data="upcoming_month_8"),
            InlineKeyboardButton("Сен", callback_data="upcoming_month_9")
        ])
        keyboard.append([
            InlineKeyboardButton("Окт", callback_data="upcoming_month_10"),
            InlineKeyboardButton("Ноя", callback_data="upcoming_month_11"),
            InlineKeyboardButton("Дек", callback_data="upcoming_month_12")
        ])
        keyboard.append([
            InlineKeyboardButton("◀️ Назад", callback_data="admin_movies")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=reply_markup)
        return
    
    elif data.startswith("upcoming_month_"):
        month = int(data.replace("upcoming_month_", ""))
        from datetime import datetime
        
        current_year = datetime.now().year
        
        month_names = {
            1: "январь", 2: "февраль", 3: "март", 4: "апрель",
            5: "май", 6: "июнь", 7: "июль", 8: "август",
            9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь"
        }
        
        await query.edit_message_text(f"🔍 Загружаю ожидаемые новинки за {month_names[month]} {current_year}...")
        
        movies = admin.get_upcoming_premieres(year=current_year, month=month, limit=10)
        
        if not movies:
            text = f"⭐ <b>Ожидаемые новинки за {month_names[month]} {current_year}</b>\n\n"
            text += "Фильмы не найдены 🥺"
        else:
            text = f"⭐ <b>Топ-10 ожидаемых новинок за {month_names[month]} {current_year}</b>\n\n"
            
            for i, movie in enumerate(movies, 1):
                kp_url = f"https://www.kinopoisk.ru/film/{movie['id']}/"
                rating_display = f"★ {movie['rating']:.1f}" if movie['rating'] else "нет рейтинга"
                text += f"{i}. <a href='{kp_url}'><b>{movie['name']}</b></a> ({movie['year']})\n"
                text += f"   {rating_display} | 👥 {movie['await_count']} ожиданий\n"
                text += f"   Премьера: {movie['premiere_date']}\n\n"
            
            # Проверяем длину сообщения
            if len(text) > 3500:
                text = text[:3500] + "...\n\n<i>Сообщение слишком длинное, показаны не все фильмы</i>"
        
        keyboard = [
            [InlineKeyboardButton("📅 Выбрать другой месяц", callback_data="admin_movies_upcoming")],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_movies")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            text, 
            parse_mode='HTML', 
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
        return
    
    elif data == "admin_movies_opinions":
        # Просмотр мнений (заглушка, сделаем позже)
        text = "💭 <b>Просмотр мнений</b>\n\nРаздел в разработке. Здесь можно будет:\n"
        text += "• Искать мнения по ID фильма\n"
        text += "• Редактировать мнения\n"
        text += "• Смотреть историю изменений"
        
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_movies")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=reply_markup)
        return
    
    elif data == "admin_opinions":
        text = "💭 <b>Управление мнениями</b>\n\nРаздел в разработке"
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=reply_markup)
        return
    
    elif data == "admin_feedback":
        text = "📝 <b>Обращения пользователей</b>\n\nРаздел в разработке"
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=reply_markup)
        return
    
    elif data == "admin_stats":
        text = "📊 <b>Статистика</b>\n\nРаздел в разработке"
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=reply_markup)
        return

async def handle_filter_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий на кнопки фильтров"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    # Получаем поисковый запрос из контекста
    query_text = context.user_data.get('query', '')
    if not query_text:
        # Если запрос не найден, пробуем восстановить из callback_data
        # Это запасной вариант
        await query.edit_message_text("❌ Поисковый запрос утерян. Начни поиск заново.")
        return
    
    if data == "noop":
        return
    
    if data == "new_search":
        await query.message.reply_text("🐾 Введи название фильма для поиска:")
        await query.message.delete()
        return
    
    if data.startswith("movie_page_"):
        # Обработка пагинации при показе карточек
        page = int(data.replace("movie_page_", ""))
        movies_list = context.user_data.get('movies', [])
        
        if movies_list:
            await show_movies_page(update, context, movies_list, query_text, page)
        else:
            await query.edit_message_text("❌ Результаты поиска устарели. Начни поиск заново.")
        return
    
    # Определяем действие по callback_data
    if data.startswith("filter_toggle_rating_"):
        # Toggle рейтинга
        value = data.replace("filter_toggle_rating_", "").split('_')[0]
        filters = context.user_data.get('search_filters', {}).copy()
        
        if filters.get('rating_range') == value:
            filters.pop('rating_range', None)
        else:
            filters['rating_range'] = value
        
        context.user_data['search_filters'] = filters
    
    elif data.startswith("filter_toggle_decade_"):
        # Toggle десятилетия
        value = data.replace("filter_toggle_decade_", "").split('_')[0]
        filters = context.user_data.get('search_filters', {}).copy()
        
        if filters.get('decade') == value:
            filters.pop('decade', None)
        else:
            filters['decade'] = value
        
        context.user_data['search_filters'] = filters
    
    elif data.startswith("filter_reset_all_"):
        # Сброс всех фильтров
        context.user_data['search_filters'] = {}
        filters = {}
    
    elif data.startswith("filter_show_results_"):
        # Показ результатов
        movies_list = context.user_data.get('full_movies_list', [])
        
        if movies_list:
            context.user_data['movies'] = movies_list
            context.user_data['current_index'] = 0
            await show_movies_page(update, context, movies_list, query_text, 0)
        else:
            await query.edit_message_text("❌ Нет фильмов для показа")
        return
    
    else:
        return
    
    # Получаем актуальные фильтры
    filters = context.user_data.get('search_filters', {})
    
    # Показываем сообщение о подсчете
    await query.edit_message_text("🔍 Применяю фильтры...")
    
    # Получаем только количество фильмов и флаг "есть ещё"
    total_count, has_more = movie.search_movies_with_filters(
        query_text, 
        filters=filters if filters else None, 
        count_only=True
    )
    
    # Сохраняем полный список для последующего показа
    if total_count > 0:
        full_list = movie.search_movies_with_filters(
            query_text, 
            filters=filters if filters else None, 
            count_only=False
        )
        context.user_data['full_movies_list'] = full_list
    else:
        context.user_data.pop('full_movies_list', None)
        context.user_data.pop('movies', None)
    
    # Формируем текст о количестве
    if has_more:
        count_text = f">{total_count}"
    else:
        count_text = str(total_count)
    
    # Показываем интерфейс с фильтрами
    text = f"🔍 <b>Поиск: {query_text}</b>\n\n"
    if filters:
        text += "Активные фильтры:\n"
        if filters.get('rating_range'):
            rating_names = {
                'new': '🆕 Новинки',
                '5-6': '⭐ 5-6',
                '6-7': '⭐ 6-7',
                '7-8': '⭐ 7-8',
                '8-9': '⭐ 8-9',
                '9-10': '⭐ 9-10'
            }
            text += f"• {rating_names.get(filters['rating_range'], filters['rating_range'])}\n"
        if filters.get('decade'):
            decade_names = {
                'pre1980': '📽 До 1980',
                '1980s': '📅 1980-е',
                '1990s': '📅 1990-е',
                '2000s': '📅 2000-е',
                '2010s': '📅 2010-е',
                '2020s': '📅 2020-е'
            }
            text += f"• {decade_names.get(filters['decade'], filters['decade'])}\n"
        text += "\n"
    
    text += f"Найдено фильмов: <b>{count_text}</b>\n\n"
    text += "Настрой фильтры и нажми 'Показать карточки'"
    
    filter_keyboard = movie.format_filter_keyboard(
        query_text, 
        filters if filters else None, 
        total_count,
        has_more
    )
    
    await query.edit_message_text(
        text, 
        parse_mode='HTML', 
        reply_markup=filter_keyboard,
        disable_web_page_preview=True
    )
        
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админ-панель"""
    user_obj = update.message.from_user
    
    if not admin.is_admin(user_obj.id):
        await update.message.reply_text("🐾 Эта команда только для моих тренеров!")
        return
    
    keyboard = [
        [InlineKeyboardButton("👥 Пользователи", callback_data="admin_users")],
        [InlineKeyboardButton("🎬 Фильмы", callback_data="admin_movies")],
        [InlineKeyboardButton("💭 Мнения", callback_data="admin_opinions")],
        [InlineKeyboardButton("📝 Обращения", callback_data="admin_feedback")],
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("❌ Закрыть", callback_data="admin_close")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🐕‍🦺 <b>Панель управления КиноИщейки</b>\n\nВыбери раздел:",
        parse_mode='HTML',
        reply_markup=reply_markup
    )

async def show_movies_page(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                           movies_list, query_text, page=0):
    """Показывает страницу с 5 фильмами"""
    items_per_page = 5
    total_pages = (len(movies_list) + items_per_page - 1) // items_per_page
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(movies_list))
    
    message = update.callback_query.message if update.callback_query else update.message
    
    # Определяем, является ли это поиском по персонам
    is_person_search = context.user_data.get('is_person_search', False)
    
    # Отправляем заголовок
    await message.reply_text(
        f"📽 <b>Результаты поиска \"{query_text}\"</b>\n"
        f"Страница {page+1} из {total_pages}\n"
        f"Показаны фильмы {start_idx+1}-{end_idx}",
        parse_mode='HTML'
    )
    
    # Отправляем фильмы
    for movie_data in movies_list[start_idx:end_idx]:
        # Используем format_movie_card с правильными параметрами
        card, reply_markup = movie.format_movie_card(
            movie_data, 
            is_premiers=False, 
            query=query_text, 
            is_person_search=is_person_search
        )
        if card:
            try:
                await message.reply_text(card, parse_mode='HTML', reply_markup=reply_markup)
            except Exception as e:
                logger.error(f"Ошибка показа фильма: {e}")
    
    # Кнопки навигации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Предыдущая", callback_data=f"movie_page_{page-1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("Следующая ➡️", callback_data=f"movie_page_{page+1}"))
    
    if nav_buttons:
        keyboard = [nav_buttons]
        keyboard.append([InlineKeyboardButton("🔍 Новый поиск", callback_data="new_search")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await message.reply_text("Что дальше?", reply_markup=reply_markup)
    else:
        await message.reply_text("🏁 Все фильмы показаны. Начни новый поиск командой /search")

async def show_filtered_movies(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                movies_list, query_text, filters, page=0):
    """Показывает результаты поиска с фильтрами"""
    
    items_per_page = 1  # Показываем по одному фильму
    total_pages = len(movies_list)
    
    if page >= total_pages:
        page = total_pages - 1
    
    movie_data = movies_list[page]
    card, _ = movie.format_movie_card(movie_data)
    
    if not card:
        await update.callback_query.edit_message_text("Ошибка отображения фильма")
        return
    
    # Создаем клавиатуру
    keyboard = []
    
    # Кнопки навигации
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️", callback_data=f"movie_page_{page-1}"))
    nav_row.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("▶️", callback_data=f"movie_page_{page+1}"))
    if nav_row:
        keyboard.append(nav_row)
    
    # Кнопка "Мнение"
    if movie_data.get('id'):
        keyboard.append([InlineKeyboardButton(
            "🎬 Мнение КиноИщейки", 
            callback_data=f"ai:{movie_data['id']}:{movie_data.get('year', '')}"
        )])
    
    # Добавляем клавиатуру с фильтрами
    filter_keyboard = movie.format_filter_keyboard(query_text, filters, total_pages)
    
    # Объединяем клавиатуры - преобразуем tuple в list
    full_keyboard = keyboard + list(filter_keyboard.inline_keyboard)
    
    reply_markup = InlineKeyboardMarkup(full_keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            card, 
            parse_mode='HTML', 
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
    else:
        await update.message.reply_text(
            card, 
            parse_mode='HTML', 
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )

# ==================== MAIN ====================

def main():
    # Используем кастомный request вместо стандартного
    application = Application.builder() \
        .token(TELEGRAM_TOKEN) \
        .request(custom_request) \
        .build()

    # Обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("about", about))
    application.add_handler(CommandHandler("random", random_movie))
    application.add_handler(CommandHandler("search", search))
    application.add_handler(CommandHandler("premiers", premiers))
    application.add_handler(CommandHandler("person", person))
    application.add_handler(CommandHandler("tariff", show_tariff_info))
    application.add_handler(CommandHandler("feedback", feedback))
    application.add_handler(CommandHandler("faq", faq))
    application.add_handler(CommandHandler("admin", admin_panel))

    # Обработчики кнопок
    application.add_handler(CallbackQueryHandler(handle_feedback, pattern="^feedback_"))
    application.add_handler(CallbackQueryHandler(handle_feedback_pagination, pattern="^feedback_(prev|next)$"))
    application.add_handler(CallbackQueryHandler(handle_faq_button, pattern="^faq_"))
    application.add_handler(CallbackQueryHandler(handle_donate_button, pattern="^donate$"))
    application.add_handler(CallbackQueryHandler(handle_donate_amount, pattern="^donate_"))
    application.add_handler(CallbackQueryHandler(handle_check_payment, pattern="^check_payment_"))
    application.add_handler(CallbackQueryHandler(handle_donate_amount, pattern="^skip_email$"))
    application.add_handler(CallbackQueryHandler(lambda update, ctx: update.callback_query.edit_message_text("🐾 Поняла, сворачиваю информацию о платеже!"), pattern="^payment_close$"))
    application.add_handler(CallbackQueryHandler(handle_admin_callback, pattern="^admin_"))
    application.add_handler(CallbackQueryHandler(handle_admin_callback, pattern="^anniversary_month_"))
    application.add_handler(CallbackQueryHandler(handle_admin_callback, pattern="^upcoming_month_"))
    application.add_handler(CallbackQueryHandler(handle_admin_callback, pattern="^admin_|^anniversary_month_|^upcoming_month_|^export_"))
    application.add_handler(CallbackQueryHandler(handle_filter_callback, pattern="^filter_|^noop$|^new_search$|^movie_page_"))
    application.add_handler(CallbackQueryHandler(handle_button_click))

    # Обработчик текста (должен быть последним!)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, process_feedback_text))

    application.run_polling()


if __name__ == '__main__':
    main()
