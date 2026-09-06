# bot_card.py
import os
import re
import json
import logging
import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# ==================== КОНФИГУРАЦИЯ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

import configparser
config = configparser.ConfigParser(interpolation=None)
config.read(CONFIG_PATH)

CARD_BOT_TOKEN = config['CardBot']['token']
OPENAI_API_KEY = config['OpenAI']['api_key']
OPENAI_BASE_URL = config['OpenAI']['base_url']
DEEPSEEK_URL = f"{OPENAI_BASE_URL}/v1/chat/completions"

# ==================== НАСТРОЙКА ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

ALLOWED_USER_ID = 397469639

# ==================== ФУНКЦИИ ПАРСИНГА ====================

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


# ==================== КОМАНДА ГЕНЕРАЦИИ ====================

async def generate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /generate - генерирует карточки из текста"""
    user_id = update.message.from_user.id
    
    if user_id != ALLOWED_USER_ID:
        await update.message.reply_text("❌ Доступ запрещен")
        return
    
    # Проверяем, есть ли текст после команды
    text = ' '.join(context.args)
    
    if not text:
        await update.message.reply_text(
            "❌ Использование: /generate <текст фильма и мнение>\n\n"
            "Пример:\n"
            "/generate Название фильма (2024)\n"
            "Страна: Россия\n"
            "Режиссер: Иван Иванов\n"
            "Актеры: Петр Петров\n\n"
            "Отзыв о фильме...\n"
            "Оценка: 8\n"
            "Настроение: #классно #интересно\n"
            "Атмосфера: #тёмная #атмосферная"
        )
        return
    
    await update.message.reply_text("🔄 Генерирую карточки...")
    
    try:
        # Разделяем текст на части
        parts = text.split('\n\n')
        
        if len(parts) < 2:
            await update.message.reply_text(
                "❌ Неправильный формат. Должны быть две части:\n"
                "1. Информация о фильме\n"
                "2. Мнение о фильме\n\n"
                "Разделяй их пустой строкой."
            )
            return
        
        # Парсим фильм и мнение
        film_text = parts[0]
        opinion_text = parts[1]
        
        film_data = parse_film_text(film_text)
        opinion_data = parse_opinion_text(opinion_text)
        
        if not film_data['title']:
            await update.message.reply_text("❌ Не удалось распознать название фильма")
            return
        
        if not opinion_data['opinion']:
            await update.message.reply_text("❌ Не удалось распознать мнение о фильме")
            return
        
        # Разбиваем мнение на блоки
        blocks = split_opinion_with_ai(
            opinion_data["opinion"],
            opinion_data["rating"],
            opinion_data["hashtags"],
            opinion_data["atmosphere_hashtags"]
        )
        
        # Форматируем результат
        result = format_cards_output(film_data, opinion_data, blocks)
        
        # Отправляем результат
        await update.message.reply_text(result, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text(f"❌ Произошла ошибка: {str(e)}")


async def generate_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка reply на команду /generate"""
    user_id = update.message.from_user.id
    
    if user_id != ALLOWED_USER_ID:
        await update.message.reply_text("❌ Доступ запрещен")
        return
    
    # Если это ответ на команду /generate
    if update.message.reply_to_message and update.message.reply_to_message.text:
        if '/generate' in update.message.reply_to_message.text:
            text = update.message.text
            await generate(update, context)
            return


# ==================== КОМАНДЫ ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    user_id = update.message.from_user.id
    
    if user_id != ALLOWED_USER_ID:
        await update.message.reply_text("❌ Доступ запрещен")
        return
    
    await update.message.reply_text(
        "🐕 <b>КиноИщейка - генератор карточек</b>\n\n"
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
        parse_mode='HTML'
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


# ==================== MAIN ====================

def main():
    """Запуск бота"""
    print("🐕 КиноИщейка - генератор карточек")
    print("=" * 40)
    print(f"Токен: {CARD_BOT_TOKEN[:10]}...")
    print(f"API URL: {OPENAI_BASE_URL}")
    print("=" * 40)
    
    # Создаем приложение
    application = Application.builder().token(CARD_BOT_TOKEN).build()
    
    # Добавляем команды
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("generate", generate))
    
    # Обработчик для текстовых сообщений (реплай на generate)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, generate_reply))
    
    # Запускаем бота
    print("🚀 Бот запущен! Ожидаю команды...")
    print("Для остановки нажмите Ctrl+C")
    print("=" * 40)
    
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
