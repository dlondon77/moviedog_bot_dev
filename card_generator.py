# bot.py
import os
import re
import json
import httpx
import requests
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

if not TOKEN:
    print("❌ Ошибка: переменная CARD_BOT_TOKEN не установлена")
    exit(1)
if not DEEPSEEK_API_KEY:
    print("❌ Ошибка: переменная OPENAI_API_KEY не установлена")
    exit(1)

print(f"✅ Токен: {TOKEN[:10]}...")
print(f"✅ API Key: {DEEPSEEK_API_KEY[:10]}...")

# ==================== СОСТОЯНИЯ ====================
WAITING_FOR_FILM, WAITING_FOR_OPINION = range(2)

DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"

# ==================== ФУНКЦИИ ПАРСИНГА ====================
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

# ==================== ГЕНЕРАЦИЯ СЛАЙДОВ ====================

def split_opinion_with_deepseek(text, rating, hashtags, atmosphere_hashtags):
    """Разбивает мнение на 5 блоков через DeepSeek"""
    
    prompt = f"""Ты — КиноИщейка, собака-девочка, кинокритик. Разбей мнение на 5 блоков для слайдов.

Мнение:
{text}

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
        headers = {
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json"
        }
        
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

def format_cards_output(film_data, opinion_data, blocks):
    """Форматирует вывод карточек"""
    slide_titles = ["О чём лай?", "Какая атмосфера?", "Какая игра?", "Что зарыто?", "Какой вердикт?"]
    
    result = f"🐕 <b>Карточки для фильма: {film_data['title']}</b>\n\n"
    
    # Информация о фильме
    result += "📋 <b>Информация о фильме:</b>\n"
    result += f"🎬 Название: {film_data['title']}\n"
    if film_data.get('year'):
        result += f"📅 Год: {film_data['year']}\n"
    if film_data.get('country'):
        result += f"🌍 Страна: {film_data['country']}\n"
    if film_data.get('director'):
        result += f"🎭 Режиссёр: {film_data['director']}\n"
    if film_data.get('actors'):
        result += f"👥 Актеры: {film_data['actors']}\n"
    
    result += "\n📝 <b>Слайды:</b>\n"
    
    for i, block in enumerate(blocks):
        result += f"\n<b>{i+1}. {slide_titles[i]}</b>\n{block}\n"
    
    result += f"\n⭐ <b>Оценка:</b> {opinion_data['rating']}/10"
    
    if opinion_data['hashtags']:
        result += f"\n🏷️ <b>Настроение:</b> {' '.join(opinion_data['hashtags'])}"
    
    if opinion_data['atmosphere_hashtags']:
        result += f"\n🌄 <b>Атмосфера:</b> {' '.join(opinion_data['atmosphere_hashtags'])}"
    
    return result

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
    film_data = context.user_data.get('film_data', {})
    
    await update.message.reply_text("🔄 Генерирую слайды...")
    
    try:
        # Разбиваем на 5 блоков
        blocks = split_opinion_with_deepseek(
            opinion_data["opinion"],
            opinion_data["rating"],
            opinion_data["hashtags"],
            opinion_data["atmosphere_hashtags"]
        )
        
        # Форматируем результат
        result = format_cards_output(film_data, opinion_data, blocks)
        
        # Кнопки
        keyboard = [
            [InlineKeyboardButton("🔄 Сгенерировать заново", callback_data="regenerate")],
            [InlineKeyboardButton("📝 Начать заново", callback_data="restart")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Отправляем
        if len(result) > 4000:
            for i in range(0, len(result), 4000):
                await update.message.reply_text(result[i:i+4000], parse_mode='HTML')
            await update.message.reply_text("📌 Что дальше?", reply_markup=reply_markup)
        else:
            await update.message.reply_text(result, parse_mode='HTML', reply_markup=reply_markup)
        
        context.user_data.clear()
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")
    
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("❌ Отменено. /start")
    return ConversationHandler.END

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка кнопок"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data == "regenerate":
        # Берем данные из контекста (если есть)
        film_data = context.user_data.get('film_data')
        opinion_data = context.user_data.get('opinion_data')
        
        if not film_data or not opinion_data:
            await query.message.reply_text("❌ Нет данных. Начни с /start")
            return
        
        await query.message.reply_text("🔄 Перегенерирую...")
        
        try:
            blocks = split_opinion_with_deepseek(
                opinion_data["opinion"],
                opinion_data["rating"],
                opinion_data["hashtags"],
                opinion_data["atmosphere_hashtags"]
            )
            
            result = format_cards_output(film_data, opinion_data, blocks)
            
            keyboard = [
                [InlineKeyboardButton("🔄 Сгенерировать заново", callback_data="regenerate")],
                [InlineKeyboardButton("📝 Начать заново", callback_data="restart")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            if len(result) > 4000:
                for i in range(0, len(result), 4000):
                    await query.message.reply_text(result[i:i+4000], parse_mode='HTML')
            else:
                await query.message.reply_text(result, parse_mode='HTML', reply_markup=reply_markup)
            
        except Exception as e:
            await query.message.reply_text(f"❌ Ошибка: {str(e)}")
    
    elif data == "restart":
        context.user_data.clear()
        await query.message.reply_text("📝 Нажми /start чтобы начать заново")

# ==================== СОЗДАЕМ ПРИЛОЖЕНИЕ ====================
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
    },
    fallbacks=[CommandHandler("cancel", cancel)]
)
app.add_handler(conv_handler)
app.add_handler(CallbackQueryHandler(button_callback, pattern="^(regenerate|restart)$"))

# ==================== ЗАПУСК ====================
print("🚀 Бот запущен!")
app.run_polling()
