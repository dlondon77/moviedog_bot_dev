# test_bot.py
import os
import re
import configparser
import requests
from flask import Flask, request, jsonify

# ==================== КОНФИГ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

config = configparser.ConfigParser(interpolation=None)
config.read(CONFIG_PATH)

def get_config_value(section, key):
    value = config[section][key]
    if value.startswith('%') and value.endswith('%'):
        env_var = value.strip('%')
        return os.environ.get(env_var, value)
    return value

TOKEN = get_config_value('CardBot', 'token')
OPENAI_API_KEY = get_config_value('OpenAI', 'api_key')
OPENAI_BASE_URL = get_config_value('OpenAI', 'base_url')
DEEPSEEK_URL = f"{OPENAI_BASE_URL}/v1/chat/completions"

ALLOWED_USER_ID = 397469639

print("🐕 КиноИщейка - генератор карточек")
print("=" * 40)
print(f"Токен: {TOKEN[:10]}..." if TOKEN else "❌ Токен не найден")

# ==================== FLASK ====================
app = Flask(__name__)

def send_message(chat_id, text, parse_mode=None):
    """Отправка сообщения"""
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = {"chat_id": chat_id, "text": text}
    if parse_mode:
        data["parse_mode"] = parse_mode
    try:
        requests.post(url, json=data, timeout=10)
    except Exception as e:
        print(f"❌ Ошибка отправки: {e}")

# ==================== ПРОСТЕЙШИЙ ПАРСИНГ ====================

def parse_film_text(text):
    """Парсит текст фильма"""
    result = {"title": "", "year": "", "country": "", "director": "", "actors": ""}
    lines = text.strip().split('\n')
    
    for i, line in enumerate(lines):
        line = line.strip()
        if i == 0:
            # Первая строка - название
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

def split_opinion_simple(text):
    """Простое разбиение текста на 5 частей (без AI)"""
    # Разбиваем по предложениям
    sentences = re.split(r'[.!?]', text)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 10]
    
    # Если меньше 5 предложений - дублируем
    while len(sentences) < 5:
        sentences.append(sentences[-1] if sentences else "Нет данных")
    
    # Берем первые 5
    return sentences[:5]

# ==================== ОБРАБОТЧИКИ ====================

@app.route('/')
def index():
    return "🐕 КиноИщейка бот работает!"

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.get_json()
        print(f"📩 Получено: {data}")
        
        if 'message' in data:
            msg = data['message']
            chat_id = msg['chat']['id']
            text = msg.get('text', '')
            user_id = msg['from']['id']
            
            # Проверка доступа
            if user_id != ALLOWED_USER_ID:
                send_message(chat_id, "❌ Доступ запрещен")
                return jsonify({"status": "ok"})
            
            # Обработка команд
            if text == '/start':
                send_message(chat_id, 
                    "🐕 <b>КиноИщейка - генератор карточек</b>\n\n"
                    "Команды:\n"
                    "/start - показать это сообщение\n"
                    "/generate - сгенерировать карточки\n"
                    "/help - помощь",
                    parse_mode='HTML'
                )
            
            elif text == '/help':
                send_message(chat_id,
                    "🐕 <b>Помощь</b>\n\n"
                    "Используй /generate с текстом:\n"
                    "<code>/generate Название (2024)\n"
                    "Страна: Россия\n"
                    "Режиссер: Иван Иванов\n"
                    "Актеры: Петр Петров\n\n"
                    "Отзыв о фильме...\n"
                    "Оценка: 8</code>",
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
                    opinion_text = sections[1]
                    
                    if not film_data['title']:
                        send_message(chat_id, "❌ Не удалось распознать название фильма")
                        return jsonify({"status": "ok"})
                    
                    # Разбиваем мнение на 5 частей (простое разбиение)
                    blocks = split_opinion_simple(opinion_text)
                    
                    # Формируем ответ
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
                    
                    send_message(chat_id, response, parse_mode='HTML')
                    
                except Exception as e:
                    send_message(chat_id, f"❌ Ошибка: {str(e)}")
            
            else:
                send_message(chat_id, 
                    f"🐕 Неизвестная команда. Используй /start или /help\n\n"
                    f"Ты написал: {text}"
                )
        
        return jsonify({"status": "ok"})
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return jsonify({"status": "error"}), 500

@app.route('/setwebhook')
def set_webhook():
    host = request.host
    webhook_url = f"https://{host}/webhook"
    url = f"https://api.telegram.org/bot{TOKEN}/setWebhook"
    response = requests.post(url, json={"url": webhook_url})
    return jsonify({"webhook_url": webhook_url, "response": response.json()})

@app.route('/deletewebhook')
def delete_webhook():
    url = f"https://api.telegram.org/bot{TOKEN}/deleteWebhook"
    response = requests.get(url)
    return jsonify(response.json())

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    print(f"\n🚀 Запуск на порту {port}")
    print("=" * 40)
    app.run(host='0.0.0.0', port=port)
