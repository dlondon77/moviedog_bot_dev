# bot_card.py
import os
import re
import json
import configparser
import requests
import logging

# ==================== КОНФИГУРАЦИЯ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

# Читаем конфиг
config = configparser.ConfigParser(interpolation=None)
config.read(CONFIG_PATH)

# Получаем токены
TOKEN = config['CardBot']['token']
OPENAI_API_KEY = config['OpenAI']['api_key']
OPENAI_BASE_URL = config['OpenAI']['base_url']
DEEPSEEK_URL = f"{OPENAI_BASE_URL}/v1/chat/completions"

ALLOWED_USER_ID = 397469639

# ==================== ЛОГИРОВАНИЕ ====================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# ==================== ОБРАБОТКА WEBHOOK (для btohost) ====================

from flask import Flask, request, jsonify

app = Flask(__name__)

# Хранилище состояний (для простоты, в продакшене используйте БД)
user_data = {}

@app.route('/webhook', methods=['POST'])
def webhook():
    """Обработка входящих сообщений от Telegram"""
    try:
        data = request.get_json()
        
        if 'message' in data:
            message = data['message']
            chat_id = message['chat']['id']
            user_id = message['from']['id']
            text = message.get('text', '')
            
            # Проверка доступа
            if user_id != ALLOWED_USER_ID:
                send_message(chat_id, "❌ Доступ запрещен")
                return jsonify({"status": "ok"})
            
            # Обработка команд
            if text.startswith('/start'):
                send_message(chat_id, 
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
            
            elif text.startswith('/generate'):
                # Извлекаем текст после команды
                parts = text.split(' ', 1)
                if len(parts) < 2:
                    send_message(chat_id, "❌ Напиши текст после /generate")
                    return jsonify({"status": "ok"})
                
                content = parts[1]
                send_message(chat_id, "🔄 Генерирую карточки...")
                
                try:
                    # Разделяем на фильм и мнение
                    sections = content.split('\n\n')
                    if len(sections) < 2:
                        send_message(chat_id, "❌ Раздели фильм и мнение пустой строкой")
                        return jsonify({"status": "ok"})
                    
                    film_data = parse_film_text(sections[0])
                    opinion_data = parse_opinion_text(sections[1])
                    
                    if not film_data['title']:
                        send_message(chat_id, "❌ Не удалось распознать название фильма")
                        return jsonify({"status": "ok"})
                    
                    if not opinion_data['opinion']:
                        send_message(chat_id, "❌ Не удалось распознать мнение о фильме")
                        return jsonify({"status": "ok"})
                    
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
                    
                    # Отправляем результат
                    send_message(chat_id, response, parse_mode='HTML')
                    
                except Exception as e:
                    logger.error(f"Ошибка: {e}")
                    send_message(chat_id, f"❌ Ошибка: {str(e)}")
        
        return jsonify({"status": "ok"})
        
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return jsonify({"status": "error"}), 500

def send_message(chat_id, text, parse_mode=None):
    """Отправка сообщения через Telegram API"""
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": text,
    }
    if parse_mode:
        data["parse_mode"] = parse_mode
    
    try:
        response = requests.post(url, json=data, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения: {e}")
        return None

@app.route('/')
def index():
    return "🐕 КиноИщейка бот работает!"

if __name__ == '__main__':
    print("🐕 КиноИщейка - генератор карточек")
    print("=" * 40)
    print(f"Токен: {TOKEN[:10]}...")
    print(f"API URL: {OPENAI_BASE_URL}")
    print("=" * 40)
    
    # Запускаем Flask сервер
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
