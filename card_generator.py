# bot.py
import os
import re
import json
import base64
import httpx
import requests
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters, ConversationHandler
from telegram.request import HTTPXRequest

# ==================== ПАТЧ ДЛЯ ПРОКСИ ====================
original_init = httpx.AsyncClient.__init__
def patched_init(self, *args, **kwargs):
    if 'proxies' in kwargs:
        del kwargs['proxies']
    original_init(self, *args, **kwargs)
httpx.AsyncClient.__init__ = patched_init

# ==================== ТОКЕНЫ ====================
TOKEN = os.environ.get('CARD_BOT_TOKEN')
DEEPSEEK_API_KEY = os.environ.get('OPENAI_API_KEY')

if not TOKEN or not DEEPSEEK_API_KEY:
    print("❌ Ошибка: установите CARD_BOT_TOKEN и OPENAI_API_KEY")
    exit(1)

print(f"✅ Токен: {TOKEN[:10]}...")
print(f"✅ API Key: {DEEPSEEK_API_KEY[:10]}...")

# ==================== ПУТИ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRAMES_DIR = os.path.join(BASE_DIR, 'frames')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
TEMPLATES_DIR = os.path.join(BASE_DIR, 'templates')

os.makedirs(FRAMES_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TEMPLATES_DIR, exist_ok=True)

# ==================== СОСТОЯНИЯ ====================
WAITING_FOR_FILM, WAITING_FOR_OPINION, WAITING_FOR_FRAMES = range(3)

DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"

# ==================== ФУНКЦИИ ====================

def get_template():
    template_path = os.path.join(TEMPLATES_DIR, 'opinion.html')
    with open(template_path, 'r', encoding='utf-8') as f:
        return f.read()

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
    result["hashtags"] = list(dict.fromkeys(result["hashtags"]))
    result["atmosphere_hashtags"] = list(dict.fromkeys(result["atmosphere_hashtags"]))
    return result

def split_opinion_with_deepseek(text, rating, hashtags, atmosphere_hashtags):
    prompt = f"""Ты — КиноИщейка, собака-девочка, кинокритик. Разбей мнение на 5 блоков для слайдов.

Мнение: {text}

Слайды:
1. "О чём лай?" — о сюжете
2. "Какая атмосфера?" — об атмосфере
3. "Какая игра?" — об актёрах
4. "Что зарыто?" — о смыслах
5. "Какой вердикт?" — итог

ПРАВИЛА:
- Каждый блок — 1-2 предложения
- Говори о себе в женском роде
- НЕ добавляй новые факты
- В 5-м блоке НЕ упоминай оценку

Оценка: {rating}/10 (НЕ УПОМИНАТЬ!)
Настроение: {" ".join(hashtags)}
Атмосфера: {" ".join(atmosphere_hashtags)}

Верни ТОЛЬКО JSON:
{{"blocks": ["блок1", "блок2", "блок3", "блок4", "блок5"]}}"""

    try:
        headers = {"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"}
        data = {
            "model": "deepseek-chat",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 600
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
        
        return fallback_split(text)
        
    except Exception as e:
        print(f"Ошибка AI: {e}")
        return fallback_split(text)

def fallback_split(text):
    sentences = re.split(r'[.!?]', text)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 10]
    while len(sentences) < 5:
        sentences.append(sentences[-1] if sentences else "Нет данных")
    return [sentences[i] + "." for i in range(5)]

def generate_card_html(frame_path, film_data, slide_title, slide_text, rating, hashtags, atmosphere_hashtags, is_first, is_atmosphere, is_last):
    html = get_template()
    
    with open(frame_path, 'rb') as f:
        img_data = base64.b64encode(f.read()).decode('utf-8')
    
    country_year = film_data["country"]
    if film_data.get("year"):
        country_year += f", {film_data['year']}"
    
    html = html.replace("{{FRAME}}", f"data:image/jpeg;base64,{img_data}")
    html = html.replace("{{SLIDE_TITLE}}", slide_title)
    html = html.replace("{{SLIDE_TEXT}}", slide_text)
    html = html.replace("{{RUBRIC}}", "Мнение КиноИщейки")
    html = html.replace("{{ARROW_VISIBLE}}", "" if is_first else "hidden")
    
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
        html = html.replace("{{RATING}}", f'<div class="rating-wrapper"><span class="rating-number">{rating}</span><span class="rating-bones">{bones}</span></div>')
    else:
        html = html.replace("{{RATING}}", "")
    
    if is_atmosphere and atmosphere_hashtags:
        html = html.replace("{{HASHTAGS}}", f'<div class="hashtags">{" ".join(atmosphere_hashtags[:5])}</div>')
    elif is_last and hashtags:
        html = html.replace("{{HASHTAGS}}", f'<div class="hashtags">{" ".join(hashtags[:5])}</div>')
    else:
        html = html.replace("{{HASHTAGS}}", "")
    
    return html

# ==================== АСИНХРОННАЯ КОНВЕРТАЦИЯ ====================

async def html_to_image_async(html_content, output_path):
    """Асинхронная конвертация HTML в изображение"""
    try:
        from playwright.async_api import async_playwright
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(viewport={'width': 1080, 'height': 1080})
            await page.set_content(html_content)
            await page.wait_for_timeout(1000)
            await page.screenshot(path=output_path, full_page=True)
            await browser.close()
        return True
    except Exception as e:
        print(f"Ошибка конвертации: {e}")
        return False

# ==================== КОМАНДЫ ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[InlineKeyboardButton("📝 Создать карточки", callback_data="create_cards")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🐕 <b>КиноИщейка - генератор карточек</b>\n\n"
        "Нажми кнопку чтобы начать:",
        parse_mode='HTML',
        reply_markup=reply_markup
    )

async def create_cards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    context.user_data['frames'] = []
    
    await query.edit_message_text(
        "📝 <b>Шаг 1: Отправь текст о фильме</b>\n\n"
        "Пример:\n"
        "<code>🎬 Как избежать наказания за убийство (2014)\n"
        "🌍 Страна: США\n"
        "🎥 Режиссер: Лора Иннес\n"
        "👥 Актеры: Чарли Уэбер, Лиза Вейл</code>\n\n"
        "Или /cancel",
        parse_mode='HTML'
    )
    return WAITING_FOR_FILM

async def handle_film(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    film_data = parse_film_text(text)
    
    if not film_data['title']:
        await update.message.reply_text("❌ Не распознано. Попробуй еще или /cancel")
        return WAITING_FOR_FILM
    
    context.user_data['film_data'] = film_data
    
    await update.message.reply_text(
        f"✅ Фильм: <b>{film_data['title']}</b>\n\n"
        "📝 <b>Шаг 2: Отправь текст мнения</b>\n\n"
        "Пример:\n"
        "<code>Отличный фильм!\n"
        "Оценка: 9\n"
        "Настроение: #классно #интересно\n"
        "Атмосфера: #тёмная #атмосферная</code>\n\n"
        "Или /cancel",
        parse_mode='HTML'
    )
    return WAITING_FOR_OPINION

async def handle_opinion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    opinion_data = parse_opinion_text(text)
    
    if not opinion_data['opinion']:
        await update.message.reply_text("❌ Не распознано. Попробуй еще или /cancel")
        return WAITING_FOR_OPINION
    
    context.user_data['opinion_data'] = opinion_data
    
    await update.message.reply_text(
        f"✅ Мнение распознано!\n\n"
        f"⭐ Оценка: {opinion_data['rating']}/10\n"
        f"🏷️ Настроение: {' '.join(opinion_data['hashtags'])}\n"
        f"🌄 Атмосфера: {' '.join(opinion_data['atmosphere_hashtags'])}\n\n"
        "📸 <b>Шаг 3: Отправь 5 кадров из фильма</b>\n"
        "Отправляй изображения по одному (нужно 5 штук)\n\n"
        "Или /cancel",
        parse_mode='HTML'
    )
    return WAITING_FOR_FRAMES

async def handle_frame(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if 'frames' not in context.user_data:
        context.user_data['frames'] = []
    
    photo = update.message.photo[-1]
    file = await photo.get_file()
    
    frame_num = len(context.user_data['frames']) + 1
    frame_path = os.path.join(FRAMES_DIR, f"frame_{frame_num}.jpg")
    await file.download_to_drive(frame_path)
    context.user_data['frames'].append(frame_path)
    
    await update.message.reply_text(f"📸 Кадр {frame_num}/5 получен!")
    
    if len(context.user_data['frames']) == 5:
        await update.message.reply_text("🔄 Все кадры получены! Генерирую карточки...")
        
        try:
            film_data = context.user_data['film_data']
            opinion_data = context.user_data['opinion_data']
            frame_paths = context.user_data['frames']
            
            blocks = split_opinion_with_deepseek(
                opinion_data["opinion"],
                opinion_data["rating"],
                opinion_data["hashtags"],
                opinion_data["atmosphere_hashtags"]
            )
            
            slide_titles = ["О чём лай?", "Какая атмосфера?", "Какая игра?", "Что зарыто?", "Какой вердикт?"]
            
            for i in range(5):
                html = generate_card_html(
                    frame_path=frame_paths[i],
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
                
                output_path = os.path.join(OUTPUT_DIR, f"card_{i+1}.png")
                
                # Используем АСИНХРОННУЮ версию
                success = await html_to_image_async(html, output_path)
                
                if success:
                    with open(output_path, 'rb') as f:
                        await update.message.reply_photo(f, caption=f"Слайд {i+1}: {slide_titles[i]}")
                    os.remove(output_path)
                else:
                    await update.message.reply_text(f"❌ Ошибка при генерации слайда {i+1}")
            
            for path in frame_paths:
                if os.path.exists(path):
                    os.remove(path)
            
            await update.message.reply_text("🎉 Готово! Все 5 карточек созданы!\nНажми /start чтобы начать заново")
            
            context.user_data.clear()
            
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка: {str(e)}")
        
        return ConversationHandler.END
    
    return WAITING_FOR_FRAMES

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("❌ Отменено. /start")
    return ConversationHandler.END

# ==================== ПРИЛОЖЕНИЕ ====================
custom_client = httpx.AsyncClient(
    timeout=httpx.Timeout(30.0),
    follow_redirects=True
)

custom_request = HTTPXRequest(
    connection_pool_size=1,
    connect_timeout=30.0,
    read_timeout=30.0,
    write_timeout=30.0,
    pool_timeout=30.0
)
custom_request._client = custom_client

app = Application.builder().token(TOKEN).request(custom_request).build()

# ==================== ОБРАБОТЧИКИ ====================
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("cancel", cancel))

conv_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(create_cards, pattern="^create_cards$")],
    states={
        WAITING_FOR_FILM: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_film)],
        WAITING_FOR_OPINION: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_opinion)],
        WAITING_FOR_FRAMES: [MessageHandler(filters.PHOTO, handle_frame)],
    },
    fallbacks=[CommandHandler("cancel", cancel)]
)
app.add_handler(conv_handler)

# ==================== ЗАПУСК ====================
print("🚀 Бот запущен!")
print(f"📁 Шаблон: {os.path.join(TEMPLATES_DIR, 'opinion.html')}")
app.run_polling()
