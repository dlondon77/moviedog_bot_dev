# card_bot.py
import os
import re
import json
import configparser
import requests
import logging
from flask import Flask, request, jsonify
from datetime import datetime

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

# ==================== ЛОГИРОВАНИЕ ====================
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
print(f"Токен: {TOKEN[:10]}..." if TOKEN else "❌ Токен не найден")
print("=" * 40)

# ==================== FLASK ====================
app = Flask(__name__)

# Хранилище состояний пользователей (временно, в памяти)
user_sessions = {}

def send_message(chat_id, text, parse_mode=None, reply_markup=None):
    """Отправка сообщения"""
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = {"chat_id": chat_id, "text": text}
    if parse_mode:
        data["parse_mode"] = parse_mode
    if reply_markup:
        data["reply_markup"] = reply_markup
    try:
        response = requests.post(url, json=data, timeout=10)
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
        return None

def send_keyboard(chat_id, text, buttons, parse_mode=None):
    """Отправка сообщения с клавиатурой"""
    reply_markup = {
        "keyboard": buttons,
        "resize_keyboard": True,
        "one_time_keyboard": True
    }
    return send_message(chat_id, text, parse_mode, reply_markup)

# ==================== ПАРСИНГ ====================

def parse_film_text(text):
    """Парсит текст фильма из сообщения"""
    result = {
        "title": "",
        "type": "",
        "rating": "",
        "country": "",
        "genre": "",
        "description": "",
        "director": "",
        "actors": ""
    }
    
    lines = text.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Убираем эмодзи в начале
        line_clean = re.sub(r'^[🎬📁⭐🌍🎭📝🎥👥]', '', line).strip()
        
        if "Тип:" in line_clean:
            result["type"] = line_clean.replace("Тип:", "").strip()
        elif "Рейтинг Кинопоиска:" in line_clean:
            result["rating"] = line_clean.replace("Рейтинг Кинопоиска:", "").strip()
        elif "Страна:" in line_clean:
            result["country"] = line_clean.replace("Страна:", "").strip()
        elif "Жанр:" in line_clean:
            result["genre"] = line_clean.replace("Жанр:", "").strip()
        elif "Описание:" in line_clean:
            result["description"] = line_clean.replace("Описание:", "").strip()
        elif "Режиссер" in line_clean or "Режиссёр" in line_clean:
            result["director"] = line_clean.replace("Режиссер:", "").replace("Режиссёр:", "").strip()
        elif "Актеры" in line_clean or "Актёры" in line_clean:
            result["actors"] = line_clean.replace("Актеры:", "").replace("Актёры:", "").strip()
        else:
            # Если строка без метки - это может быть название
            if not result["title"] and not any(key in line_clean for key in ["Тип:", "Рейтинг:", "Страна:", "Жанр:", "Описание:", "Режиссер", "Актеры"]):
                # Пробуем извлечь год из названия
                year_match = re.search(r'\((\d{4})\)', line_clean)
                if year_match:
                    result["title"] = re.sub(r'\s*\(\d{4}\)$', '', line_clean).strip()
                else:
                    result["title"] = line_clean
    
    return result

def parse_opinion_text(text):
    """Парсит текст мнения"""
    result = {
        "opinion": "",
        "rating": 0,
        "rating_comment": "",
        "hashtags": [],
        "atmosphere_hashtags": []
    }
    
    lines = text.strip().split('\n')
    opinion_parts = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        if line.startswith("Оценка:"):
            # Парсим оценку
            match = re.search(r'(\d+)\s*(?:из\s*10)?', line)
            if match:
                result["rating"] = int(match.group(1))
            # Остаток строки после оценки - комментарий
            rating_comment = re.sub(r'^Оценка:\s*\d+\s*(?:из\s*10)?\s*\.?\s*', '', line)
            if rating_comment:
                result["rating_comment"] = rating_comment
        elif line.startswith("Настроение:"):
            result["hashtags"].extend(re.findall(r'#[А-Яа-яA-Za-z_\w]+', line))
        elif line.startswith("Атмосфера:"):
            result["atmosphere_hashtags"].extend(re.findall(r'#[А-Яа-яA-Za-z_\w]+', line))
        elif line.startswith("🐾") or line.startswith("Я уже смотрела"):
            # Пропускаем служебные строки
            continue
        else:
            opinion_parts.append(line)
    
    result["opinion"] = ' '.join(opinion_parts).strip()
    return result

def split_opinion_with_ai(text, rating, hashtags, atmosphere_hashtags):
    """Разбивает мнение на 5 блоков через AI"""
    
    prompt = f"""Ты — КиноИщейка, собака-девочка, кинокритик с отличным чутьём. Ты уже написала своё мнение о фильме, теперь тебе нужно разбить его на 5 смысловых блоков для слайдов в Instagram-карусели.

Твоё мнение:
{text}

Разбей его на 5 логических блоков для следующих слайдов:
1️⃣ "О чём лай?" — кратко о сюжете, главном герое, что происходит
2️⃣ "Какая атмосфера?" — описание атмосферы, визуала, настроения фильма
3️⃣ "Какая игра?" — об актёрской игре, кто особенно запомнился
4️⃣ "Что зарыто?" — о скрытых смыслах, глубине, идеях, символизме
5️⃣ "Какой вердикт?" — итоговое мнение, плюсы и минусы, стоит ли смотреть

ПРАВИЛА:
- Каждый блок — 1-2 предложения (максимум 30 слов)
- Сохрани собачий юмор и образ КиноИщейки (говори о себе в женском роде)
- НЕ добавляй новые факты
- В 5-м блоке НЕ упоминай оценку числом

Оценка: {rating}/10 (НЕ упоминай в тексте блоков!)
Хэштеги настроения: {" ".join(hashtags) if hashtags else "нет"}
Хэштеги атмосферы: {" ".join(atmosphere_hashtags) if atmosphere_hashtags else "нет"}

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
    if opinion_data['rating_comment']:
        result += f" ({opinion_data['rating_comment']})"
    
    if opinion_data['hashtags']:
        result += f"\n🏷️ <b>Настроение:</b> {' '.join(opinion_data['hashtags'])}"
    
    if opinion_data['atmosphere_hashtags']:
        result += f"\n🌄 <b>Атмосфера:</b> {' '.join(opinion_data['atmosphere_hashtags'])}"
    
    return result

# ==================== ОБРАБОТЧИКИ ====================

@app.route('/')
def index():
    return "🐕 КиноИщейка - генератор карточек работает!"

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.get_json()
        logger.info(f"Получено: {data}")
        
        if 'message' in data:
            msg = data['message']
            chat_id = msg['chat']['id']
            user_id = msg['from']['id']
            text = msg.get('text', '')
            
            # Проверка доступа
            if user_id != ALLOWED_USER_ID:
                send_message(chat_id, "❌ Доступ запрещен")
                return jsonify({"status": "ok"})
            
            # Получаем или создаем сессию пользователя
            if chat_id not in user_sessions:
                user_sessions[chat_id] = {
                    'stage': 'idle',  # idle, waiting_film, waiting_opinion
                    'film_data': None,
                    'opinion_data': None
                }
            
            session = user_sessions[chat_id]
            
            # Обработка команд
            if text == '/start':
                session['stage'] = 'idle'
                send_message(chat_id,
                    "🐕 <b>КиноИщейка - генератор карточек</b>\n\n"
                    "Я помогу тебе разбить мнение о фильме на 5 слайдов.\n\n"
                    "📌 <b>Как это работает:</b>\n"
                    "1. Сначала отправь мне <b>текст о фильме</b>\n"
                    "2. Затем отправь <b>текст мнения</b>\n"
                    "3. Я сгенерирую 5 красивых слайдов\n\n"
                    "Нажми /new чтобы начать\n"
                    "Или /cancel чтобы отменить",
                    parse_mode='HTML'
                )
                return jsonify({"status": "ok"})
            
            elif text == '/new':
                session['stage'] = 'waiting_film'
                session['film_data'] = None
                session['opinion_data'] = None
                send_message(chat_id,
                    "📝 <b>Шаг 1 из 2: Отправь текст о фильме</b>\n\n"
                    "Пример из вашего файла:\n"
                    "<code>🎬 Как избежать наказания за убийство (2014)\n"
                    "📁 Тип: сериал\n"
                    "⭐ Рейтинг Кинопоиска: 8.1\n"
                    "🌍 Страна: США\n"
                    "🎭 Жанр: детектив, триллер, криминал, драма\n"
                    "📝 Описание: Профессор Эннализ Китинг...\n"
                    "🎥 Режиссер: Лора Иннес, Майкл Смит...\n"
                    "👥 Актеры: Чарли Уэбер, Лиза Вейл...</code>\n\n"
                    "Или отправь /cancel чтобы отменить",
                    parse_mode='HTML'
                )
                return jsonify({"status": "ok"})
            
            elif text == '/cancel':
                session['stage'] = 'idle'
                session['film_data'] = None
                session['opinion_data'] = None
                send_message(chat_id, "❌ Операция отменена. Нажми /new чтобы начать заново")
                return jsonify({"status": "ok"})
            
            elif text == '/help':
                send_message(chat_id,
                    "🐕 <b>Помощь</b>\n\n"
                    "Команды:\n"
                    "/start - показать приветствие\n"
                    "/new - начать создание карточек\n"
                    "/cancel - отменить текущую операцию\n"
                    "/help - показать это сообщение\n\n"
                    "Как это работает:\n"
                    "1. Нажми /new\n"
                    "2. Отправь текст о фильме\n"
                    "3. Отправь текст мнения\n"
                    "4. Получи 5 готовых слайдов!",
                    parse_mode='HTML'
                )
                return jsonify({"status": "ok"})
            
            # Обработка пошагового сбора
            elif session['stage'] == 'waiting_film':
                # Получаем текст фильма
                film_data = parse_film_text(text)
                
                if not film_data['title']:
                    send_message(chat_id,
                        "❌ Не удалось распознать название фильма.\n"
                        "Убедись, что первая строка - это название.\n\n"
                        "Попробуй еще раз или /cancel для отмены"
                    )
                    return jsonify({"status": "ok"})
                
                session['film_data'] = film_data
                session['stage'] = 'waiting_opinion'
                
                # Показываем что распознали
                preview = (
                    f"✅ <b>Распознано:</b>\n\n"
                    f"🎬 Название: {film_data['title']}\n"
                )
                if film_data.get('year'):
                    preview += f"📅 Год: {film_data['year']}\n"
                if film_data.get('country'):
                    preview += f"🌍 Страна: {film_data['country']}\n"
                if film_data.get('director'):
                    preview += f"🎭 Режиссёр: {film_data['director'][:100]}...\n"
                if film_data.get('actors'):
                    preview += f"👥 Актеры: {film_data['actors'][:100]}...\n"
                
                preview += (
                    f"\n📝 <b>Шаг 2 из 2: Отправь текст мнения</b>\n\n"
                    "Пример из вашего файла:\n"
                    "<code>Я уже смотрела Как избежать наказания за убийство (2014), и вот что думаю:\n\n"
                    "Этот сериал я вынюхала с первого эпизода...\n\n"
                    "Оценка: 9 из 10\n\n"
                    "Настроение: #Напряжение #Интрига #Хитрость\n"
                    "Атмосфера: #Мрачность #Умность #Шик</code>\n\n"
                    "Или /cancel для отмены"
                )
                
                send_message(chat_id, preview, parse_mode='HTML')
                return jsonify({"status": "ok"})
            
            elif session['stage'] == 'waiting_opinion':
                # Получаем текст мнения
                opinion_data = parse_opinion_text(text)
                
                if not opinion_data['opinion']:
                    send_message(chat_id,
                        "❌ Не удалось распознать мнение.\n"
                        "Убедись, что текст содержит отзыв.\n\n"
                        "Попробуй еще раз или /cancel для отмены"
                    )
                    return jsonify({"status": "ok"})
                
                session['opinion_data'] = opinion_data
                
                # Показываем что распознали
                preview = (
                    f"✅ <b>Мнение распознано</b>\n\n"
                    f"⭐ Оценка: {opinion_data['rating']}/10\n"
                )
                if opinion_data['rating_comment']:
                    preview += f"📝 Комментарий: {opinion_data['rating_comment']}\n"
                if opinion_data['hashtags']:
                    preview += f"🏷️ Настроение: {' '.join(opinion_data['hashtags'])}\n"
                if opinion_data['atmosphere_hashtags']:
                    preview += f"🌄 Атмосфера: {' '.join(opinion_data['atmosphere_hashtags'])}\n"
                
                preview += "\n🔄 <b>Генерирую карточки...</b>"
                send_message(chat_id, preview, parse_mode='HTML')
                
                # Генерируем карточки
                try:
                    blocks = split_opinion_with_ai(
                        opinion_data["opinion"],
                        opinion_data["rating"],
                        opinion_data["hashtags"],
                        opinion_data["atmosphere_hashtags"]
                    )
                    
                    film_data = session['film_data']
                    result = format_cards_output(film_data, opinion_data, blocks)
                    
                    # Добавляем кнопки в конце
                    keyboard = {
                        "inline_keyboard": [
                            [{"text": "🔄 Сгенерировать заново", "callback_data": "regenerate"}],
                            [{"text": "📝 Начать заново", "callback_data": "restart"}]
                        ]
                    }
                    
                    # Отправляем результат
                    if len(result) > 4000:
                        for i in range(0, len(result), 4000):
                            send_message(chat_id, result[i:i+4000], parse_mode='HTML')
                        send_message(chat_id, "📌 <b>Что дальше?</b>", parse_mode='HTML', reply_markup=json.dumps(keyboard))
                    else:
                        send_message(chat_id, result, parse_mode='HTML', reply_markup=json.dumps(keyboard))
                    
                    # Сохраняем данные для регенерации
                    session['stage'] = 'idle'
                    
                except Exception as e:
                    logger.error(f"Ошибка генерации: {e}")
                    send_message(chat_id, f"❌ Ошибка при генерации: {str(e)}")
                    session['stage'] = 'idle'
                
                return jsonify({"status": "ok"})
            
            else:
                send_message(chat_id,
                    "🐕 Нажми /new чтобы начать создание карточек\n"
                    "Или /help для справки"
                )
        
        return jsonify({"status": "ok"})
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        return jsonify({"status": "error"}), 500

# ==================== ОБРАБОТКА КНОПОК ====================

@app.route('/callback', methods=['POST'])
def callback():
    """Обработка нажатий на кнопки"""
    try:
        data = request.get_json()
        logger.info(f"Callback: {data}")
        
        if 'callback_query' in data:
            query = data['callback_query']
            chat_id = query['message']['chat']['id']
            user_id = query['from']['id']
            callback_data = query['data']
            
            # Проверка доступа
            if user_id != ALLOWED_USER_ID:
                send_message(chat_id, "❌ Доступ запрещен")
                return jsonify({"status": "ok"})
            
            if callback_data == 'regenerate':
                # TODO: регенерация с теми же данными
                send_message(chat_id, "🔄 Функция регенерации в разработке...")
            
            elif callback_data == 'restart':
                if chat_id in user_sessions:
                    user_sessions[chat_id]['stage'] = 'idle'
                    user_sessions[chat_id]['film_data'] = None
                    user_sessions[chat_id]['opinion_data'] = None
                send_message(chat_id, "📝 Нажми /new чтобы начать заново")
        
        return jsonify({"status": "ok"})
        
    except Exception as e:
        logger.error(f"Ошибка callback: {e}")
        return jsonify({"status": "error"}), 500

@app.route('/setwebhook')
def set_webhook():
    """Установка webhook"""
    host = request.host
    webhook_url = f"https://{host}/webhook"
    
    # Также устанавливаем callback URL
    callback_url = f"https://{host}/callback"
    
    url = f"https://api.telegram.org/bot{TOKEN}/setWebhook"
    response = requests.post(url, json={"url": webhook_url})
    
    return jsonify({
        "webhook_url": webhook_url,
        "callback_url": callback_url,
        "response": response.json()
    })

@app.route('/deletewebhook')
def delete_webhook():
    """Удаление webhook"""
    url = f"https://api.telegram.org/bot{TOKEN}/deleteWebhook"
    response = requests.get(url)
    return jsonify(response.json())

# ==================== ЗАПУСК ====================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    print(f"\n🚀 Запуск на порту {port}")
    print(f"📌 Установи webhook: https://ваш-домен.btohost.com/setwebhook")
    print("=" * 40)
    app.run(host='0.0.0.0', port=port)
