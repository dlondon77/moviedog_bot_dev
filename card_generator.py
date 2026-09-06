# bot_card.py
import os
import re
import json
import configparser
import requests
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ==================== ОТКЛЮЧАЕМ ПРОКСИ ====================
# Удаляем все переменные прокси
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

# ==================== ЛОГИРОВАНИЕ ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

print("🐕 КиноИщейка - генератор карточек")
print("=" * 40)
print(f"Токен: {TOKEN[:10]}...")
print(f"API URL: {OPENAI_BASE_URL}")
print("=" * 40)

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
        logger.error(f"Ошибка AI: {e}")
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
        
        if len(response) > 4000:
            for i in range(0, len(response), 4000):
                await update.message.reply_text(response[i:i+4000], parse_mode='HTML')
        else:
            await update.message.reply_text(response, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")

# ==================== ЗАПУСК ====================

def main():
    """Запуск бота"""
    try:
        # Пробуем создать приложение стандартным способом
        app = Application.builder().token(TOKEN).build()
        
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("generate", generate))
        
        print("🚀 Бот запущен через long polling!")
        print(f"Разрешенный пользователь: {ALLOWED_USER_ID}")
        print("=" * 40)
        
        app.run_polling(allowed_updates=Update.ALL_TYPES)
        
    except TypeError as e:
        if 'proxies' in str(e):
            logger.error("Ошибка с прокси, пробуем обход...")
            # Если ошибка с прокси - используем старый метод
            from telegram.ext import Updater
            
            updater = Updater(token=TOKEN)
            dp = updater.dispatcher
            
            dp.add_handler(CommandHandler("start", start))
            dp.add_handler(CommandHandler("generate", generate))
            
            print("🚀 Бот запущен через Updater (обход прокси)!")
            updater.start_polling()
            updater.idle()
        else:
            raise

if __name__ == "__main__":
    main()
