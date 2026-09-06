# card_generator_bot.py
import os
import re
import json
import configparser
import logging
import requests
import httpx
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    CallbackQueryHandler,
)
from telegram.request import HTTPXRequest

# ==================== ПОЛНОЕ ОТКЛЮЧЕНИЕ ПРОКСИ ====================
# 1. Удаляем все возможные переменные прокси из окружения
for env_var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(env_var, None)

# 2. Патчим httpx.AsyncClient, чтобы игнорировать параметр 'proxies'
original_init = httpx.AsyncClient.__init__
def patched_init(self, *args, **kwargs):
    if 'proxies' in kwargs:
        del kwargs['proxies']  # Удаляем проблемный параметр
    original_init(self, *args, **kwargs)
httpx.AsyncClient.__init__ = patched_init

# 3. Для синхронного клиента тоже делаем патч
original_sync_init = httpx.Client.__init__
def patched_sync_init(self, *args, **kwargs):
    if 'proxies' in kwargs:
        del kwargs['proxies']
    original_sync_init(self, *args, **kwargs)
httpx.Client.__init__ = patched_sync_init

# ==================== КОНФИГУРАЦИЯ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

config = configparser.ConfigParser(interpolation=None)
config.read(CONFIG_PATH)

# Получаем токены с поддержкой %VAR%
def get_config_value(section, key):
    value = config[section][key]
    if value.startswith('%') and value.endswith('%'):
        env_var = value.strip('%')
        return os.environ.get(env_var, value)
    return value

TELEGRAM_TOKEN = get_config_value('CardBot', 'token')
OPENAI_API_KEY = get_config_value('OpenAI', 'api_key')
OPENAI_BASE_URL = get_config_value('OpenAI', 'base_url')
DEEPSEEK_URL = f"{OPENAI_BASE_URL}/v1/chat/completions"

if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_TOKEN не найден!")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY не найден!")

ALLOWED_USER_ID = 397469639

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
LOG_PATH = os.path.join(BASE_DIR, 'logs', 'card_bot.log')
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

print("🐕 КиноИщейка - генератор карточек")
print("=" * 40)
print(f"Токен: {TELEGRAM_TOKEN[:10]}...")
print(f"API URL: {OPENAI_BASE_URL}")
print("=" * 40)

# ==================== СОЗДАЕМ ПРИЛОЖЕНИЕ ====================
# Используем кастомный клиент, как в рабочем скрипте
try:
    # Создаем кастомный клиент без прокси
    custom_client = httpx.AsyncClient(
        timeout=httpx.Timeout(30.0),
        limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        follow_redirects=True
    )
    
    # Создаем кастомный HTTPXRequest
    custom_request = HTTPXRequest(
        connection_pool_size=1,
        connect_timeout=30.0,
        read_timeout=30.0,
        write_timeout=30.0,
        pool_timeout=30.0
    )
    # Подменяем внутренний клиент
    custom_request._client = custom_client
    
    # Создаем приложение с кастомным запросом
    application = Application.builder().token(TELEGRAM_TOKEN).request(custom_request).build()
    logger.info("✅ Приложение создано с кастомным клиентом")
    
except Exception as e:
    logger.error(f"❌ Ошибка создания приложения с кастомным клиентом: {e}")
    # Fallback - пробуем стандартный способ
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    logger.info("✅ Приложение создано стандартным способом")

# ==================== ФУНКЦИИ ====================

def parse_film_text(text):
    """Парсит текст фильма"""
    result = {
        "title": "",
        "year": "",
        "country": "",
        "director": "",
        "actors": ""
    }
    
    lines = text.strip().split('\n')
    
    for i, line in enumerate(lines):
        line = line.strip()
        
        if i == 0:
            title_clean = re.sub(r'^[🎬📁⭐🌍🎭📝🎥👥]', '', line).strip()
            year_match = re.search(r'\((\d{4})\)', title_clean)
            if year_match:
                result["year"] = year_match.group(1)
                result["title"] = re.sub(r'\s*\(\d{4}\)$', '', title_clean).strip()
            else:
                result["title"] = title_clean
            continue
        
        line_clean = re.sub(r'^[🎬📁⭐🌍🎭📝🎥👥]', '', line).strip()
        
        if "Страна:" in line_clean:
            result["country"] = line_clean.replace("Страна:", "").strip()
        elif "Режиссер" in line_clean or "Режиссёр" in line_clean:
            result["director"] = line_clean.replace("Режиссер:", "").replace("Режиссёр:", "").strip()
        elif "Актеры" in line_clean or "Актёры" in line_clean:
            result["actors"] = line_clean.replace("Актеры:", "").replace("Актёры:", "").strip()
    
    return result


def parse_opinion_text(text):
    """Парсит текст мнения"""
    result = {
        "opinion": "",
        "rating": 0,
        "hashtags": [],
        "atmosphere_hashtags": []
    }
    
    for line in text.strip().split("\n"):
        line = line.strip()
        if line.startswith("Оценка:"):
            match = re.search(r'(\d+)', line)
            if match:
                result["rating"] = int(match.group(1))
        elif line.startswith("Настроение:"):
            result["hashtags"].extend(re.findall(r'#[А-Яа-яA-Za-z_\w]+', line))
        elif line.startswith("Атмосфера:"):
            result["atmosphere_hashtags"].extend(re.findall(r'#[А-Яа-яA-Za-z_\w]+', line))
        else:
            result["opinion"] += line + " "
    
    result["opinion"] = result["opinion"].strip()
    result["hashtags"] = list(dict.fromkeys(result["hashtags"]))
    result["atmosphere_hashtags"] = list(dict.fromkeys(result["atmosphere_hashtags"]))
    return result


def split_opinion_with_ai(text, rating, hashtags, atmosphere_hashtags):
    """Разбивает мнение на 5 блоков через AI"""
    
    prompt = f"""Ты — КиноИщейка, собака-девочка, кинокритик. Разбей своё мнение о фильме на 5 смысловых блоков для слайдов.

Твоё мнение:
{text}

Разбей на 5 блоков для слайдов:
1️⃣ "О чём лай?" — кратко о сюжете
2️⃣ "Какая атмосфера?" — об атмосфере и визуале
3️⃣ "Какая игра?" — об актёрской игре
4️⃣ "Что зарыто?" — о скрытых смыслах
5️⃣ "Какой вердикт?" — итоговое мнение

ПРАВИЛА:
- Каждый блок — 1-2 предложения (максимум 30 слов)
- Сохрани образ КиноИщейки (говори о себе в женском роде)
- НЕ добавляй новые факты
- В 5-м блоке НЕ упоминай оценку числом

Оценка: {rating}/10 (НЕ упоминай в тексте!)
Хэштеги настроения: {" ".join(hashtags)}
Хэштеги атмосферы: {" ".join(atmosphere_hashtags)}

Верни ответ строго в формате JSON:
{{
    "blocks": [
        "текст для слайда 1",
        "текст для слайда 2",
        "текст для слайда 3",
        "текст для слайда 4",
        "текст для слайда 5"
    ]
}}"""

    try:
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": "Ты — КиноИщейка, собака-девочка, кинокритик. Отвечай только JSON."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.3,
            "max_tokens": 600
        }
        
        logger.info("Отправляем запрос в DeepSeek...")
        response = requests.post(DEEPSEEK_URL, headers=headers, json=data, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        content = result["choices"][0]["message"]["content"]
        
        logger.info("Ответ получен")
        
        json_match = re.search(r'\{.*\}', content, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group())
            blocks = data.get("blocks", [])
            if len(blocks) >= 5:
                return blocks[:5]
        
        logger.warning("Не удалось распарсить ответ, используем fallback")
        return fallback_split(text)
        
    except Exception as e:
        logger.error(f"Ошибка AI: {e}")
        return fallback_split(text)


def fallback_split(text):
    """Запасной вариант разбивки"""
    sentences = re.split(r'[.!?]', text)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 10]
    
    if len(sentences) >= 5:
        return [sentences[i] + "." for i in range(5)]
    
    while len(sentences) < 5:
        sentences.append(sentences[-1] if sentences else "Нет данных")
    
    return [sentences[i] + "." for i in range(5)]


def format_cards_output(film_data, opinion_data, blocks):
    """Форматирует вывод карточек"""
    slide_titles = [
        "О чём лай?",
        "Какая атмосфера?",
        "Какая игра?",
        "Что зарыто?",
        "Какой вердикт?"
    ]
    
    result = f"🐕 <b>Карточки для фильма: {film_data['title']}</b>\n\n"
    
    # Информация о фильме
    result += "📋 <b>Информация о фильме:</b>\n"
    result += f"🎬 Название: {film_data['title']}\n"
    if film_data['year']:
        result += f"📅 Год: {film_data['year']}\n"
    if film_data['country']:
        result += f"🌍 Страна: {film_data['country']}\n"
    if film_data['director']:
        result += f"🎭 Режиссёр: {film_data['director']}\n"
    if film_data['actors']:
        result += f"👥 Актеры: {film_data['actors']}\n"
    
    result += "\n📝 <b>Слайды:</b>\n"
    
    for i, block in enumerate(blocks):
        result += f"\n<b>{i+1}. {slide_titles[i]}</b>\n"
        result += f"{block}\n"
    
    result += f"\n⭐ <b>Оценка:</b> {opinion_data['rating']}/10"
    
    if opinion_data['hashtags']:
        result += f"\n🏷️ <b>Настроение:</b> {' '.join(opinion_data['hashtags'])}"
    
    if opinion_data['atmosphere_hashtags']:
        result += f"\n🌄 <b>Атмосфера:</b> {' '.join(opinion_data['atmosphere_hashtags'])}"
    
    return result

# ==================== КОМАНДЫ ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    user_id = update.message.from_user.id
    
    if user_id != ALLOWED_USER_ID:
        await update.message.reply_text("❌ Доступ запрещен")
        return
    
    keyboard = [
        [InlineKeyboardButton("📝 Сгенерировать карточки", callback_data="generate_start")],
        [InlineKeyboardButton("❓ Помощь", callback_data="help")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🐕 <b>КиноИщейка - генератор карточек</b>\n\n"
        "Я помогу тебе разбить мнение о фильме на 5 слайдов для Instagram.\n\n"
        "📌 <b>Команды:</b>\n"
        "/start - показать это сообщение\n"
        "/generate - сгенерировать карточки\n"
        "/help - помощь\n\n"
        "📌 <b>Как использовать /generate:</b>\n"
        "Отправь текст с информацией о фильме и мнением.\n\n"
        "Пример:\n"
        "<code>/generate Название фильма (2024)\n"
        "Страна: Россия\n"
        "Режиссер: Иван Иванов\n"
        "Актеры: Петр Петров\n\n"
        "Отзыв о фильме...\n"
        "Оценка: 8\n"
        "Настроение: #классно #интересно\n"
        "Атмосфера: #тёмная #атмосферная</code>",
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help"""
    user_id = update.message.from_user.id
    
    if user_id != ALLOWED_USER_ID:
        await update.message.reply_text("❌ Доступ запрещен")
        return
    
    await update.message.reply_text(
        "🐕 <b>Помощь</b>\n\n"
        "📌 <b>Команды:</b>\n"
        "/start - показать приветствие\n"
        "/generate - сгенерировать карточки\n"
        "/help - показать это сообщение\n\n"
        "📌 <b>Формат для /generate:</b>\n\n"
        "Первая часть - информация о фильме:\n"
        "Название фильма (2024)\n"
        "Страна: Россия\n"
        "Режиссер: Иван Иванов\n"
        "Актеры: Петр Петров, Анна Сидорова\n\n"
        "Вторая часть - мнение (через пустую строку):\n"
        "Текст отзыва...\n"
        "Оценка: 8\n"
        "Настроение: #классно #интересно\n"
        "Атмосфера: #тёмная #атмосферная",
        parse_mode='HTML'
    )


async def generate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /generate - генерирует карточки из текста"""
    user_id = update.message.from_user.id
    
    if user_id != ALLOWED_USER_ID:
        await update.message.reply_text("❌ Доступ запрещен")
        return
    
    text = ' '.join(context.args)
    if not text:
        await update.message.reply_text(
            "❌ Использование: /generate текст\n\n"
            "Пример:\n"
            "<code>/generate Название (2024)\n"
            "Страна: Россия\n"
            "Режиссер: Иван Иванов\n"
            "Актеры: Петр Петров\n\n"
            "Отзыв о фильме...\n"
            "Оценка: 8\n"
            "Настроение: #классно\n"
            "Атмосфера: #тёмная</code>",
            parse_mode='HTML'
        )
        return
    
    await process_generation(update, context, text)


async def process_generation(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """Процесс генерации карточек"""
    await update.message.reply_text("🔄 Генерирую карточки...")
    
    try:
        parts = text.split('\n\n')
        if len(parts) < 2:
            await update.message.reply_text("❌ Раздели фильм и мнение пустой строкой")
            return
        
        film_data = parse_film_text(parts[0])
        opinion_data = parse_opinion_text(parts[1])
        
        if not film_data['title']:
            await update.message.reply_text("❌ Не удалось распознать название фильма")
            return
        
        if not opinion_data['opinion']:
            await update.message.reply_text("❌ Не удалось распознать мнение о фильме")
            return
        
        blocks = split_opinion_with_ai(
            opinion_data["opinion"],
            opinion_data["rating"],
            opinion_data["hashtags"],
            opinion_data["atmosphere_hashtags"]
        )
        
        response = format_cards_output(film_data, opinion_data, blocks)
        
        # Добавляем кнопки
        keyboard = [
            [InlineKeyboardButton("🔄 Сгенерировать заново", callback_data="regenerate")],
            [InlineKeyboardButton("📝 Начать заново", callback_data="restart")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Отправляем результат
        if len(response) > 4000:
            for i in range(0, len(response), 4000):
                await update.message.reply_text(response[i:i+4000], parse_mode='HTML')
            await update.message.reply_text(
                "📌 <b>Что дальше?</b>",
                parse_mode='HTML',
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(response, parse_mode='HTML', reply_markup=reply_markup)
        
        # Сохраняем данные для регенерации
        context.user_data['last_film_text'] = parts[0]
        context.user_data['last_opinion_text'] = parts[1]
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text(f"❌ Произошла ошибка: {str(e)}")


# ==================== ОБРАБОТКА КНОПОК ====================

async def handle_button_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий на кнопки"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    if user_id != ALLOWED_USER_ID:
        await query.message.reply_text("❌ Доступ запрещен")
        return
    
    data = query.data
    
    if data == "generate_start":
        await query.edit_message_text(
            "🐕 Отправь текст в формате:\n\n"
            "<b>Информация о фильме:</b>\n"
            "Название фильма (2024)\n"
            "Страна: Россия\n"
            "Режиссер: Иван Иванов\n"
            "Актеры: Петр Петров\n\n"
            "<b>Мнение о фильме:</b>\n"
            "Твой отзыв...\n"
            "Оценка: 8\n"
            "Настроение: #классно #интересно\n"
            "Атмосфера: #тёмная #атмосферная",
            parse_mode='HTML'
        )
        return
    
    elif data == "help":
        await help_command(update, context)
        return
    
    elif data == "restart":
        await query.edit_message_text("🐕 Нажми /start чтобы начать заново")
        return
    
    elif data == "regenerate":
        film_text = context.user_data.get('last_film_text')
        opinion_text = context.user_data.get('last_opinion_text')
        
        if film_text and opinion_text:
            await query.edit_message_text("🔄 Перегенерирую карточки...")
            # Создаем mock-объект для update
            class MockUpdate:
                def __init__(self, message):
                    self.message = message
            mock_update = MockUpdate(query.message)
            await process_generation(mock_update, context, f"{film_text}\n\n{opinion_text}")
        else:
            await query.edit_message_text("❌ Нет данных для регенерации. Начни с /start")
        return


# ==================== ОБРАБОТКА ТЕКСТА ====================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений"""
    user_id = update.message.from_user.id
    
    if user_id != ALLOWED_USER_ID:
        await update.message.reply_text("❌ Доступ запрещен")
        return
    
    # Если пользователь просто отправил текст, предлагаем использовать /generate
    await update.message.reply_text(
        "🐕 Я вижу твое сообщение!\n\n"
        "Чтобы сгенерировать карточки, используй команду /generate\n\n"
        "Пример:\n"
        "<code>/generate Название (2024)\n"
        "Страна: Россия\n"
        "Режиссер: Иван Иванов\n"
        "Актеры: Петр Петров\n\n"
        "Отзыв о фильме...\n"
        "Оценка: 8\n"
        "Настроение: #классно\n"
        "Атмосфера: #тёмная</code>",
        parse_mode='HTML'
    )


# ==================== MAIN ====================

def main():
    """Запуск бота"""
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("generate", generate))
    
    # Обработчики кнопок
    application.add_handler(CallbackQueryHandler(handle_button_click))
    
    # Обработчик текста (должен быть последним)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    
    print("🚀 Бот запущен!")
    print(f"Разрешенный пользователь: {ALLOWED_USER_ID}")
    print("=" * 40)
    
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
