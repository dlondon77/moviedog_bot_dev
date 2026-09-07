# bot.py
import os
import re
import json
import httpx
import requests
from PIL import Image, ImageDraw, ImageFont
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler, ConversationHandler
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

os.makedirs(FRAMES_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"

# ==================== СОСТОЯНИЯ ====================
WAITING_FOR_FILM, WAITING_FOR_OPINION, WAITING_FOR_FRAMES = range(3)

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

# ==================== ГЕНЕРАЦИЯ КАРТОЧКИ ЧЕРЕЗ PILLOW ====================

def create_card_image(frame_path, output_path, slide_title, slide_text, 
                      film_data, rating, hashtags, atmosphere_hashtags,
                      is_first, is_atmosphere, is_last):
    """Создает карточку с помощью Pillow"""
    try:
        # Открываем фоновое изображение
        img = Image.open(frame_path)
        img = img.resize((1080, 1080))
        
        # Создаем градиентный overlay
        overlay = Image.new('RGBA', (1080, 1080), (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        
        # Затемнение сверху и снизу
        for y in range(1080):
            if y < 400:
                alpha = int(200 * (1 - y / 400))
                draw.rectangle([(0, y), (1080, y+1)], fill=(0, 0, 0, alpha))
            elif y > 680:
                alpha = int(200 * ((y - 680) / 400))
                draw.rectangle([(0, y), (1080, y+1)], fill=(0, 0, 0, alpha))
        
        img = Image.alpha_composite(img.convert('RGBA'), overlay)
        img = img.convert('RGB')
        
        draw = ImageDraw.Draw(img)
        
        # Пытаемся загрузить шрифты
        try:
            font_title = ImageFont.truetype('/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf', 50)
            font_text = ImageFont.truetype('/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf', 40)
            font_small = ImageFont.truetype('/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf', 30)
        except:
            font_title = ImageFont.load_default()
            font_text = ImageFont.load_default()
            font_small = ImageFont.load_default()
        
        # Верхняя часть - информация о фильме (только на первом слайде)
        if is_first:
            y = 50
            draw.text((50, y), film_data['title'], fill=(255, 215, 0), font=font_title)
            y += 60
            
            country_year = film_data['country']
            if film_data.get('year'):
                country_year += f", {film_data['year']}"
            draw.text((50, y), country_year, fill=(255, 255, 255), font=font_small)
            y += 40
            
            draw.text((50, y), f"Режиссёр: {film_data['director']}", fill=(200, 200, 200), font=font_small)
            y += 35
            draw.text((50, y), f"Актеры: {film_data['actors'][:100]}...", fill=(200, 200, 200), font=font_small)
        
        # Центр - заголовок и текст слайда
        center_y = 400 if is_first else 300
        
        # Заголовок
        bbox = draw.textbbox((0, 0), slide_title, font=font_title)
        title_width = bbox[2] - bbox[0]
        draw.text(((1080 - title_width) // 2, center_y), slide_title, fill=(255, 215, 0), font=font_title)
        
        # Текст слайда (перенос по словам)
        words = slide_text.split()
        lines = []
        current_line = []
        for word in words:
            test_line = ' '.join(current_line + [word])
            bbox = draw.textbbox((0, 0), test_line, font=font_text)
            if bbox[2] - bbox[0] < 900:
                current_line.append(word)
            else:
                lines.append(' '.join(current_line))
                current_line = [word]
        if current_line:
            lines.append(' '.join(current_line))
        
        text_y = center_y + 70
        for line in lines[:4]:  # Максимум 4 строки
            bbox = draw.textbbox((0, 0), line, font=font_text)
            text_width = bbox[2] - bbox[0]
            draw.text(((1080 - text_width) // 2, text_y), line, fill=(255, 255, 255), font=font_text)
            text_y += 50
        
        # Нижняя часть - рубрика и оценка (только на последнем)
        bottom_y = 950
        
        if is_last:
            # Рубрика
            draw.text((50, bottom_y), "Мнение КиноИщейки", fill=(214, 94, 125), font=font_small)
            
            # Оценка
            rating_text = f"⭐ {rating}/10"
            bbox = draw.textbbox((0, 0), rating_text, font=font_title)
            rating_width = bbox[2] - bbox[0]
            draw.text((1080 - rating_width - 50, bottom_y), rating_text, fill=(255, 215, 0), font=font_title)
            
            # Хэштеги настроения (под низом)
            if hashtags:
                hashtag_text = ' '.join(hashtags[:5])
                bbox = draw.textbbox((0, 0), hashtag_text, font=font_small)
                text_width = bbox[2] - bbox[0]
                draw.text(((1080 - text_width) // 2, 1020), hashtag_text, fill=(64, 167, 227), font=font_small)
        
        elif is_atmosphere and atmosphere_hashtags:
            # Хэштеги атмосферы на втором слайде
            hashtag_text = ' '.join(atmosphere_hashtags[:5])
            bbox = draw.textbbox((0, 0), hashtag_text, font=font_small)
            text_width = bbox[2] - bbox[0]
            draw.text(((1080 - text_width) // 2, 1020), hashtag_text, fill=(64, 167, 227), font=font_small)
        
        # Стрелка в правом верхнем углу (только на первом)
        if is_first:
            draw.text((1020, 30), "→", fill=(255, 255, 255), font=font_title)
        
        # Сохраняем
        img.save(output_path, 'PNG')
        return True
        
    except Exception as e:
        print(f"Ошибка создания карточки: {e}")
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
            output_paths = []
            
            for i in range(5):
                output_path = os.path.join(OUTPUT_DIR, f"card_{i+1}.png")
                
                success = create_card_image(
                    frame_path=frame_paths[i],
                    output_path=output_path,
                    slide_title=slide_titles[i],
                    slide_text=blocks[i],
                    film_data=film_data,
                    rating=opinion_data["rating"],
                    hashtags=opinion_data["hashtags"],
                    atmosphere_hashtags=opinion_data["atmosphere_hashtags"],
                    is_first=(i == 0),
                    is_atmosphere=(i == 1),
                    is_last=(i == 4)
                )
                
                if success:
                    output_paths.append(output_path)
                else:
                    await update.message.reply_text(f"❌ Ошибка при генерации слайда {i+1}")
            
            if output_paths:
                media_group = []
                for i, path in enumerate(output_paths):
                    with open(path, 'rb') as f:
                        media_group.append(
                            InputMediaPhoto(
                                media=f,
                                caption=f"Слайд {i+1}: {slide_titles[i]}" if i == 0 else None
                            )
                        )
                    os.remove(path)
                
                await update.message.reply_media_group(media_group)
                await update.message.reply_text("🎉 Готово! Нажми /start чтобы начать заново")
            else:
                await update.message.reply_text("❌ Не удалось сгенерировать ни одной карточки")
            
            for path in frame_paths:
                if os.path.exists(path):
                    os.remove(path)
            
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

print("🚀 Бот запущен!")
app.run_polling()
