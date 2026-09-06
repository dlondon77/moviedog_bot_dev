# bot_card.py
import os
import re
import json
import configparser
import requests
import httpx
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from telegram.request import HTTPXRequest

# ==================== ОТКЛЮЧАЕМ ПРОКСИ ====================
for env_var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(env_var, None)

# ==================== КОНФИГУРАЦИЯ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

config = configparser.ConfigParser(interpolation=None)
config.read(CONFIG_PATH)

TOKEN = config['CardBot']['token']
OPENAI_API_KEY = config['OpenAI']['api_key']
OPENAI_BASE_URL = config['OpenAI']['base_url']
DEEPSEEK_URL = f"{OPENAI_BASE_URL}/v1/chat/completions"

ALLOWED_USER_ID = 397469639

print("🐕 Запуск бота...")
print(f"Токен: {TOKEN[:10]}...")

# ==================== СОЗДАЕМ КАСТОМНЫЙ КЛИЕНТ ====================
# Создаем HTTP клиент без прокси
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

# ==================== СОЗДАЕМ ПРИЛОЖЕНИЕ ====================
app = Application.builder().token(TOKEN).request(custom_request).build()

# ==================== ФУНКЦИИ ====================

def parse_film_text(text):
    result = {"title": "", "year": "", "country": "", "director": "", "actors": ""}
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
        else:
            line_clean = re.sub(r'^[🎬📁⭐🌍🎭📝🎥👥]', '', line).strip()
            if "Страна:" in line_clean:
                result["country"] = line_clean.replace("Страна:", "").strip()
            elif "Режиссер" in line_clean or "Режиссёр" in line_clean:
                result["director"] = line_clean.replace("Режиссер:", "").replace("Режиссёр:", "").strip()
            elif "Актеры" in line_clean or "Актёры" in line_clean:
                result["actors"] = line_clean.replace("Актеры:", "").replace("Актёры:", "").strip()
    
    return result

def parse_opinion_text(text):
    result = {"opinion": "", "rating": 0, "hashtags": [], "atmosphere_hashtags": []}
    
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
    return result

def split_opinion_with_ai(text):
    prompt = f"""Разбей это мнение на 5 блоков для слайдов.
Верни ТОЛЬКО JSON.

Мнение: {text}

Формат:
{{"blocks": ["блок 1", "блок 2", "блок 3", "блок 4", "блок 5"]}}"""

    try:
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        data = {
            "model": "deepseek-chat",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 500
        }
        
        response = requests.post(DEEPSEEK_URL, headers=headers, json=data, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        content = result["choices"][0]["message"]["content"]
        
        json_match = re.search(r'\{.*\}', content, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group())
            blocks = data.get("blocks", [])
            if len(blocks) >= 5:
                return blocks[:5]
        
        return [f"Блок {i+1}" for i in range(5)]
        
    except Exception as e:
        print(f"Ошибка AI: {e}")
        return [f"Блок {i+1}" for i in range(5)]

# ==================== КОМАНДЫ ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ALLOWED_USER_ID:
        await update.message.reply_text("❌ Доступ запрещен")
        return
    
    await update.message.reply_text(
        "🐕 <b>КиноИщейка - генератор карточек</b>\n\n"
        "Используй /generate с текстом:\n"
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

async def generate(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        
        blocks = split_opinion_with_ai(opinion_data["opinion"])
        
        response = f"🐕 <b>Карточки для фильма: {film_data['title']}</b>\n\n"
        
        # Информация о фильме
        response += "📋 <b>Информация о фильме:</b>\n"
        response += f"🎬 Название: {film_data['title']}\n"
        if film_data['year']:
            response += f"📅 Год: {film_data['year']}\n"
        if film_data['country']:
            response += f"🌍 Страна: {film_data['country']}\n"
        if film_data['director']:
            response += f"🎭 Режиссёр: {film_data['director']}\n"
        if film_data['actors']:
            response += f"👥 Актеры: {film_data['actors']}\n"
        
        response += "\n📝 <b>Слайды:</b>\n"
        
        titles = ["О чём лай?", "Какая атмосфера?", "Какая игра?", "Что зарыто?", "Какой вердикт?"]
        for i, block in enumerate(blocks):
            response += f"\n<b>{i+1}. {titles[i]}</b>\n{block}\n"
        
        response += f"\n⭐ <b>Оценка:</b> {opinion_data['rating']}/10"
        
        if opinion_data['hashtags']:
            response += f"\n🏷️ <b>Настроение:</b> {' '.join(opinion_data['hashtags'])}"
        
        if opinion_data['atmosphere_hashtags']:
            response += f"\n🌄 <b>Атмосфера:</b> {' '.join(opinion_data['atmosphere_hashtags'])}"
        
        # Отправляем результат (разбиваем если длинный)
        if len(response) > 4000:
            for i in range(0, len(response), 4000):
                await update.message.reply_text(response[i:i+4000], parse_mode='HTML')
        else:
            await update.message.reply_text(response, parse_mode='HTML')
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")

# ==================== ЗАПУСК ====================

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("generate", generate))

print("🚀 Бот запущен!")
print(f"Разрешенный пользователь: {ALLOWED_USER_ID}")
print("=" * 40)
app.run_polling()
