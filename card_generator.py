# bot_card.py
import os
import re
import base64
import json
import configparser
import logging
from datetime import datetime
import requests
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    ConversationHandler,
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

# ==================== КОНФИГУРАЦИЯ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

# Загрузка конфигурации с ОТКЛЮЧЕННОЙ интерполяцией
config = configparser.ConfigParser(interpolation=None)
config.read(CONFIG_PATH)

# Пути из конфига
OUTPUT_DIR = os.path.join(BASE_DIR, config['Data'].get('output_dir', './output'))
FRAMES_DIR = os.path.join(BASE_DIR, config['Data'].get('frames_dir', './frames'))
TEMPLATES_DIR = os.path.join(BASE_DIR, config['Data'].get('templates_dir', './templates'))
LOG_PATH = os.path.join(BASE_DIR, config['Logs'].get('log_path_cards', './logs/cards_bot.log'))

# DeepSeek API ключ из переменной окружения или конфига
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
if not OPENAI_API_KEY:
    # Если в конфиге значение с %, берем из переменной окружения
    api_key_value = config['OpenAI']['api_key']
    if api_key_value.startswith('%') and api_key_value.endswith('%'):
        # Это имя переменной окружения
        env_var_name = api_key_value.strip('%')
        OPENAI_API_KEY = os.environ.get(env_var_name)
    else:
        OPENAI_API_KEY = api_key_value

if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY не найден! Установите переменную окружения OPENAI_API_KEY")

OPENAI_BASE_URL = config['OpenAI']['base_url']
DEEPSEEK_URL = f"{OPENAI_BASE_URL}/v1/chat/completions"

# Токен бота для генерации карточек
CARD_BOT_TOKEN = os.environ.get('CARD_BOT_TOKEN')
if not CARD_BOT_TOKEN:
    # Если в конфиге значение с %, берем из переменной окружения
    token_value = config['CardBot']['token']
    if token_value.startswith('%') and token_value.endswith('%'):
        env_var_name = token_value.strip('%')
        CARD_BOT_TOKEN = os.environ.get(env_var_name)
    else:
        CARD_BOT_TOKEN = token_value

if not CARD_BOT_TOKEN:
    raise ValueError("CARD_BOT_TOKEN не найден! Установите переменную окружения CARD_BOT_TOKEN")

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== СОЗДАНИЕ ДИРЕКТОРИЙ ====================
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(FRAMES_DIR, exist_ok=True)
os.makedirs(TEMPLATES_DIR, exist_ok=True)

# ==================== СОСТОЯНИЯ ДЛЯ CONVERSATIONHANDLER ====================
WAITING_FOR_FILM = 1
WAITING_FOR_OPINION = 2
WAITING_FOR_FRAMES = 3

# ==================== ОСНОВНЫЕ ФУНКЦИИ ====================

def get_slide_titles():
    """Возвращает заголовки для слайдов"""
    return [
        "О чём лай?",
        "Какая атмосфера?",
        "Какая игра?",
        "Что зарыто?",
        "Какой вердикт?"
    ]


def parse_film_text(text):
    """Парсит текст фильма из сообщения"""
    result = {
        "title": "",
        "country": "",
        "year": "",
        "director": "",
        "actors": ""
    }
    
    lines = text.strip().split("\n")
    
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
    """Парсит текст мнения из сообщения"""
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


def split_opinion_with_deepseek(text, rating, hashtags, atmosphere_hashtags):
    """Использует DeepSeek для разбивки мнения на 5 смысловых блоков"""
    
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
- Сохрани собачий юмор и образ КиноИщейки
- Сохрани ключевые метафоры из оригинального текста
- Блоки должны быть логически связаны
- НЕ добавляй новые факты, которых нет в исходном мнении
- НЕ начинай с вводных фраз типа "Я думаю" или "Мне кажется"
- Используй только те слова и выражения, которые уже есть в тексте
- В 5-м блоке ("Какой вердикт?") НЕ упоминай оценку в виде числа

Оценка фильма: {rating}/10 (НЕ упоминай эту оценку в тексте блоков!)
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
                {"role": "system", "content": "Ты — КиноИщейка, собака-девочка, кинокритик. Ты помогаешь структурировать свои же обзоры для соцсетей. Будь точной и остроумной. Никогда не упоминай числовую оценку в тексте блоков."},
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
        
        logger.info("Ответ DeepSeek получен")
        
        json_match = re.search(r'\{.*\}', content, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group())
            blocks = data.get("blocks", [])
            if len(blocks) >= 5:
                return blocks[:5]
        
        logger.warning("Не удалось распарсить ответ DeepSeek, используем fallback")
        return fallback_split(text)
        
    except Exception as e:
        logger.error(f"Ошибка при запросе к DeepSeek: {e}")
        return fallback_split(text)


def fallback_split(text):
    """Запасной вариант разбивки текста на 5 частей"""
    sentences = re.split(r'[.!?]', text)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 10]
    
    if len(sentences) >= 5:
        return [sentences[i] + "." for i in range(5)]
    
    while len(sentences) < 5:
        sentences.append(sentences[-1] if sentences else "Нет данных")
    
    return [sentences[i] + "." for i in range(5)]


def save_template():
    """Сохраняет HTML шаблон в файл"""
    template_content = '''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>КиноИщейка - Карточка</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            background: #1a1a2e;
            font-family: 'Segoe UI', Arial, sans-serif;
        }
        .card {
            width: 1080px;
            height: 1080px;
            position: relative;
            overflow: hidden;
            background: #000;
            border-radius: 30px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.8);
        }
        .card img.background {
            width: 100%;
            height: 100%;
            object-fit: cover;
            position: absolute;
            top: 0;
            left: 0;
        }
        .overlay {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: linear-gradient(180deg, rgba(0,0,0,0.3) 0%, rgba(0,0,0,0.7) 100%);
            padding: 50px 60px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            box-sizing: border-box;
        }
        .top-section {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
        }
        .rubric {
            color: #ffd700;
            font-size: 28px;
            font-weight: bold;
            text-shadow: 0 2px 10px rgba(0,0,0,0.8);
            letter-spacing: 2px;
            background: rgba(0,0,0,0.5);
            padding: 10px 24px;
            border-radius: 50px;
            border: 2px solid #ffd700;
        }
        .arrow {
            color: #fff;
            font-size: 40px;
            opacity: 0.9;
            text-shadow: 0 2px 10px rgba(0,0,0,0.8);
            background: rgba(0,0,0,0.3);
            padding: 10px 20px;
            border-radius: 50px;
            border: 2px solid rgba(255,255,255,0.3);
        }
        .arrow.hidden { display: none; }
        
        .center-content {
            flex: 1;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            text-align: center;
            padding: 20px 0;
        }
        .slide-title {
            color: #ffd700;
            font-size: 52px;
            font-weight: bold;
            text-shadow: 0 4px 20px rgba(0,0,0,0.9);
            margin-bottom: 20px;
            letter-spacing: 1px;
        }
        .slide-text {
            color: #ffffff;
            font-size: 36px;
            line-height: 1.5;
            text-shadow: 0 2px 15px rgba(0,0,0,0.9);
            max-width: 90%;
            font-weight: 500;
        }
        
        .bottom-section {
            display: flex;
            flex-direction: column;
            gap: 12px;
        }
        .film-info {
            color: #fff;
            font-size: 22px;
            text-shadow: 0 2px 10px rgba(0,0,0,0.9);
            opacity: 0.9;
            background: rgba(0,0,0,0.5);
            padding: 12px 20px;
            border-radius: 15px;
            backdrop-filter: blur(5px);
        }
        .film-info.hidden { display: none; }
        .film-info .title {
            font-size: 30px;
            font-weight: bold;
            color: #ffd700;
            display: block;
            margin-bottom: 4px;
        }
        .film-info .details {
            font-size: 18px;
            opacity: 0.8;
        }
        
        .rating-wrapper {
            display: flex;
            align-items: center;
            gap: 20px;
            background: rgba(0,0,0,0.6);
            padding: 15px 30px;
            border-radius: 20px;
            backdrop-filter: blur(5px);
            border: 2px solid #ffd700;
        }
        .rating-number {
            color: #ffd700;
            font-size: 48px;
            font-weight: bold;
            text-shadow: 0 2px 10px rgba(0,0,0,0.8);
        }
        .rating-bones {
            font-size: 28px;
            letter-spacing: 2px;
        }
        .bone-filled { color: #ffd700; }
        .bone-empty { opacity: 0.3; filter: grayscale(1); }
        
        .hashtags {
            color: #ffd700;
            font-size: 22px;
            text-shadow: 0 2px 10px rgba(0,0,0,0.9);
            background: rgba(0,0,0,0.4);
            padding: 10px 20px;
            border-radius: 15px;
            letter-spacing: 1px;
            display: inline-block;
            backdrop-filter: blur(3px);
        }
        
        .page-number {
            color: rgba(255,255,255,0.4);
            font-size: 18px;
            position: absolute;
            bottom: 20px;
            right: 30px;
        }
    </style>
</head>
<body>
    <div class="card">
        <img class="background" src="{{FRAME}}" alt="frame">
        <div class="overlay">
            <div class="top-section">
                <div class="rubric">{{RUBRIC}}</div>
                <div class="arrow {{ARROW_VISIBLE}}">➜</div>
            </div>
            
            <div class="center-content">
                <div class="slide-title">{{SLIDE_TITLE}}</div>
                <div class="slide-text">{{SLIDE_TEXT}}</div>
            </div>
            
            <div class="bottom-section">
                <div class="film-info {{FILM_INFO_VISIBLE}}">
                    <span class="title">{{TITLE}}</span>
                    <span class="details">{{COUNTRY}} | {{DIRECTOR}}</span>
                    <span class="details" style="font-size:16px; margin-top:4px;">{{ACTORS}}</span>
                </div>
                {{RATING}}
                {{HASHTAGS}}
            </div>
        </div>
    </div>
</body>
</html>'''
    
    template_path = os.path.join(TEMPLATES_DIR, "opinion.html")
    with open(template_path, "w", encoding="utf-8") as f:
        f.write(template_content)
    logger.info(f"Шаблон сохранен в {template_path}")


def generate_card_html(frame_path, film_data, slide_title, slide_text,
                       rating, hashtags, atmosphere_hashtags, is_first, is_atmosphere, is_last):
    """Генерирует HTML для одной карточки"""
    template_path = os.path.join(TEMPLATES_DIR, "opinion.html")
    
    with open(template_path, "r", encoding="utf-8") as f:
        html = f.read()
    
    # Конвертируем изображение в base64
    with open(frame_path, "rb") as f:
        img_data = base64.b64encode(f.read()).decode("utf-8")
    
    country_year = film_data["country"]
    if film_data.get("year"):
        country_year += f", {film_data['year']}"
    
    html = html.replace("{{FRAME}}", f"data:image/jpeg;base64,{img_data}")
    html = html.replace("{{SLIDE_TITLE}}", slide_title)
    html = html.replace("{{SLIDE_TEXT}}", slide_text)
    html = html.replace("{{RUBRIC}}", "Мнение КиноИщейки")
    
    if is_first:
        html = html.replace("{{ARROW_VISIBLE}}", "")
    else:
        html = html.replace("{{ARROW_VISIBLE}}", "hidden")
    
    if is_first:
        html = html.replace("{{TITLE}}", film_data["title"])
        html = html.replace("{{COUNTRY}}", country_year)
        html = html.replace("{{DIRECTOR}}", film_data["director"])
        html = html.replace("{{ACTORS}}", film_data["actors"])
        html = html.replace("{{FILM_INFO_VISIBLE}}", "")
    else:
        html = html.replace("{{TITLE}}", "")
        html = html.replace("{{COUNTRY}}", "")
        html = html.replace("{{DIRECTOR}}", "")
        html = html.replace("{{ACTORS}}", "")
        html = html.replace("{{FILM_INFO_VISIBLE}}", "hidden")
    
    if is_last:
        bones = ""
        for i in range(10):
            if i < rating:
                bones += '<span class="bone-filled">🦴</span>'
            else:
                bones += '<span class="bone-empty">🦴</span>'
        
        rating_html = f"""
        <div class="rating-wrapper">
          <span class="rating-number">{rating}</span>
          <span class="rating-bones">{bones}</span>
        </div>
        """
        html = html.replace("{{RATING}}", rating_html)
    else:
        html = html.replace("{{RATING}}", "")
    
    if is_atmosphere and atmosphere_hashtags:
        hashtags_html = f'<div class="hashtags">{" ".join(atmosphere_hashtags[:5])}</div>'
        html = html.replace("{{HASHTAGS}}", hashtags_html)
    elif is_last and hashtags:
        hashtags_html = f'<div class="hashtags">{" ".join(hashtags[:5])}</div>'
        html = html.replace("{{HASHTAGS}}", hashtags_html)
    else:
        html = html.replace("{{HASHTAGS}}", "")
    
    return html


def html_to_image(html_content, output_path):
    """Конвертирует HTML в изображение с помощью playwright"""
    try:
        from playwright.sync_api import sync_playwright
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={'width': 1080, 'height': 1080})
            page.set_content(html_content)
            page.wait_for_timeout(1000)
            page.screenshot(path=output_path, full_page=True)
            browser.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка при конвертации HTML в изображение: {e}")
        return False


def generate_cards(film_text, opinion_text, frame_paths):
    """
    Генерирует 5 карточек-обложек
    
    Args:
        film_text (str): Текст с информацией о фильме
        opinion_text (str): Текст с мнением о фильме
        frame_paths (list): Список из 5 путей к изображениям
    
    Returns:
        list: Список путей к сгенерированным изображениям
    """
    # Проверяем, что шаблон существует
    template_path = os.path.join(TEMPLATES_DIR, "opinion.html")
    if not os.path.exists(template_path):
        save_template()
    
    # Парсим данные
    film_data = parse_film_text(film_text)
    opinion_data = parse_opinion_text(opinion_text)
    
    logger.info(f"Фильм: {film_data['title']}")
    logger.info(f"Оценка: {opinion_data['rating']}/10")
    
    # Разбиваем мнение на блоки
    blocks = split_opinion_with_deepseek(
        opinion_data["opinion"],
        opinion_data["rating"],
        opinion_data["hashtags"],
        opinion_data["atmosphere_hashtags"]
    )
    
    slide_titles = get_slide_titles()
    output_paths = []
    
    for i in range(5):
        frame_path = frame_paths[i]
        
        html = generate_card_html(
            frame_path=frame_path,
            film_data=film_data,
            slide_title=slide_titles[i],
            slide_text=blocks[i],
            rating=opinion_data["rating"],
            hashtags=opinion_data["hashtags"],
            atmosphere_hashtags=opinion_data["atmosphere_hashtags"],
            is_first=(i == 0),
            is_atmosphere=(i == 1),
            is_last=(i == 4)
        )
        
        output_path = os.path.join(OUTPUT_DIR, f"card_{i+1}_{int(datetime.now().timestamp())}.png")
        success = html_to_image(html, output_path)
        
        if success:
            output_paths.append(output_path)
            logger.info(f"Слайд {i+1} сохранен в {output_path}")
        else:
            logger.error(f"Ошибка при генерации слайда {i+1}")
            return None
    
    return output_paths


# ==================== ОБРАБОТЧИКИ КОМАНД ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    await update.message.reply_text(
        "🐕 Привет! Я КиноИщейка-дизайнер — твой помощник для создания обложек для кино-обзоров!\n\n"
        "Чтобы создать 5 карточек, мне нужно:\n"
        "1️⃣ Текст о фильме (название, страна, режиссёр, актёры)\n"
        "2️⃣ Текст мнения (оценка, настроение, атмосфера, сам отзыв)\n"
        "3️⃣ 5 кадров из фильма (по одному на каждый слайд)\n\n"
        "Давай начнём! Отправь мне текст о фильме в формате:\n\n"
        "Название фильма (2024)\n"
        "Страна: Россия\n"
        "Режиссер: Иван Иванов\n"
        "Актеры: Петр Петров, Анна Сидорова\n\n"
        "Или отправь /cancel чтобы отменить"
    )
    return WAITING_FOR_FILM


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена операции"""
    await update.message.reply_text("❌ Операция отменена. Начни заново с /start")
    return ConversationHandler.END


async def get_film_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получает текст о фильме"""
    context.user_data['film_text'] = update.message.text
    await update.message.reply_text(
        "✅ Текст о фильме получен!\n\n"
        "Теперь отправь текст с мнением о фильме в формате:\n\n"
        "Твой отзыв...\n"
        "Оценка: 8\n"
        "Настроение: #классно #интересно\n"
        "Атмосфера: #тёмная #атмосферная\n\n"
        "Или отправь /cancel чтобы отменить"
    )
    return WAITING_FOR_OPINION


async def get_opinion_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получает текст мнения"""
    context.user_data['opinion_text'] = update.message.text
    await update.message.reply_text(
        "✅ Текст мнения получен!\n\n"
        "Теперь отправь 5 кадров из фильма (изображения).\n"
        "Отправляй их по одному, я буду сохранять их в порядке получения.\n\n"
        "Отправь 5 изображений (или /cancel чтобы отменить)"
    )
    return WAITING_FOR_FRAMES


async def get_frames(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получает кадры из фильма"""
    if 'frames' not in context.user_data:
        context.user_data['frames'] = []
    
    photo = update.message.photo[-1]
    file = await photo.get_file()
    
    frame_num = len(context.user_data['frames']) + 1
    frame_path = os.path.join(FRAMES_DIR, f"frame_{frame_num}_{int(update.message.date.timestamp())}.jpg")
    await file.download_to_drive(frame_path)
    context.user_data['frames'].append(frame_path)
    
    await update.message.reply_text(f"📸 Кадр {frame_num}/5 получен!")
    
    if len(context.user_data['frames']) == 5:
        await update.message.reply_text("🎬 Все 5 кадров получены! Начинаю генерацию карточек...")
        
        try:
            # Генерируем карточки
            output_paths = generate_cards(
                film_text=context.user_data['film_text'],
                opinion_text=context.user_data['opinion_text'],
                frame_paths=context.user_data['frames']
            )
            
            if not output_paths:
                await update.message.reply_text(
                    "❌ Произошла ошибка при генерации карточек. Попробуй еще раз."
                )
                return ConversationHandler.END
            
            # Отправляем готовые карточки
            await update.message.reply_text("📤 Отправляю готовые карточки...")
            
            slide_titles = get_slide_titles()
            
            for i, path in enumerate(output_paths):
                if os.path.exists(path):
                    with open(path, "rb") as f:
                        await update.message.reply_photo(f, caption=f"Слайд {i+1}: {slide_titles[i]}")
                    # Удаляем временный файл
                    try:
                        os.remove(path)
                    except Exception as e:
                        logger.warning(f"Не удалось удалить файл {path}: {e}")
            
            await update.message.reply_text(
                "🎉 Готово! Все 5 карточек созданы!\n"
                "Хочешь создать ещё один обзор? Отправь /start"
            )
            
        except Exception as e:
            logger.error(f"Ошибка при генерации карточек: {e}", exc_info=True)
            await update.message.reply_text(
                f"❌ Произошла ошибка: {str(e)}\nПопробуй еще раз с /start"
            )
        finally:
            # Очищаем временные файлы
            for path in context.user_data.get('frames', []):
                if os.path.exists(path):
                    try:
                        os.remove(path)
                    except Exception as e:
                        logger.warning(f"Не удалось удалить файл {path}: {e}")
            
            context.user_data.clear()
            return ConversationHandler.END
    
    return WAITING_FOR_FRAMES


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help"""
    await update.message.reply_text(
        "🐕 КиноИщейка-дизайнер - помощник для создания обложек для кино-обзоров\n\n"
        "Команды:\n"
        "/start - Начать создание нового обзора\n"
        "/help - Показать это сообщение\n"
        "/cancel - Отменить текущую операцию\n\n"
        "Как это работает:\n"
        "1. Отправь текст о фильме\n"
        "2. Отправь текст мнения с оценкой и хэштегами\n"
        "3. Отправь 5 кадров из фильма\n"
        "4. Бот создаст 5 красивых карточек-обложек!"
    )


# ==================== MAIN ====================

def main():
    """Запуск бота"""
    # Сохраняем шаблон при запуске
    save_template()
    
    logger.info(f"Запуск бота-генератора карточек")
    logger.info(f"Используется конфиг: {CONFIG_PATH}")
    logger.info(f"Токен: {CARD_BOT_TOKEN[:10]}...")
    logger.info(f"OpenAI API Key: {OPENAI_API_KEY[:10]}...")
    
    # Создаем приложение с кастомным клиентом
    application = Application.builder() \
        .token(CARD_BOT_TOKEN) \
        .request(custom_request) \
        .build()
    
    # Создаем ConversationHandler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            WAITING_FOR_FILM: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_film_text)],
            WAITING_FOR_OPINION: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_opinion_text)],
            WAITING_FOR_FRAMES: [MessageHandler(filters.PHOTO, get_frames)],
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )
    
    application.add_handler(conv_handler)
    application.add_handler(CommandHandler('help', help_command))
    
    # Запускаем бота
    print("🐕 КиноИщейка-дизайнер бот запущен!")
    print(f"Используется конфиг: {CONFIG_PATH}")
    print("Для остановки нажмите Ctrl+C")
    
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
