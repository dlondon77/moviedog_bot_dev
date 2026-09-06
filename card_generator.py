# bot.py
import os
import re
import httpx
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

# ==================== ТОКЕН ====================
TOKEN = os.environ.get('CARD_BOT_TOKEN')
if not TOKEN:
    print("❌ Ошибка: переменная CARD_BOT_TOKEN не установлена")
    exit(1)

print(f"✅ Токен: {TOKEN[:10]}...")

# ==================== СОСТОЯНИЯ ====================
WAITING_FOR_FILM, WAITING_FOR_OPINION = range(2)

# ==================== ФУНКЦИИ ПАРСИНГА ====================
def parse_film_text(text):
    """Парсит текст фильма"""
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
    """Парсит текст мнения"""
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

# ==================== КОМАНДЫ ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Старт с кнопкой"""
    keyboard = [
        [InlineKeyboardButton("📝 Создать карточки", callback_data="create_cards")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🐕 <b>КиноИщейка - генератор карточек</b>\n\n"
        "Я помогу тебе разбить мнение о фильме на 5 слайдов.\n\n"
        "Нажми кнопку ниже чтобы начать:",
        parse_mode='HTML',
        reply_markup=reply_markup
    )

async def create_cards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало создания карточек"""
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text(
        "📝 <b>Шаг 1 из 2: Отправь текст о фильме</b>\n\n"
        "Пример:\n"
        "<code>🎬 Как избежать наказания за убийство (2014)\n"
        "🌍 Страна: США\n"
        "🎥 Режиссер: Лора Иннес\n"
        "👥 Актеры: Чарли Уэбер, Лиза Вейл</code>\n\n"
        "Или /cancel для отмены",
        parse_mode='HTML'
    )
    return WAITING_FOR_FILM

async def handle_film(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получаем текст фильма"""
    text = update.message.text
    film_data = parse_film_text(text)
    
    if not film_data['title']:
        await update.message.reply_text(
            "❌ Не удалось распознать название.\n"
            "Попробуй еще раз или /cancel"
        )
        return WAITING_FOR_FILM
    
    context.user_data['film_data'] = film_data
    
    await update.message.reply_text(
        f"✅ Распознано: <b>{film_data['title']}</b>\n\n"
        "📝 <b>Шаг 2 из 2: Отправь текст мнения</b>\n\n"
        "Пример:\n"
        "<code>Отличный фильм!\n"
        "Оценка: 9\n"
        "Настроение: #классно #интересно\n"
        "Атмосфера: #тёмная #атмосферная</code>\n\n"
        "Или /cancel для отмены",
        parse_mode='HTML'
    )
    return WAITING_FOR_OPINION

async def handle_opinion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получаем текст мнения"""
    text = update.message.text
    opinion_data = parse_opinion_text(text)
    
    if not opinion_data['opinion']:
        await update.message.reply_text(
            "❌ Не удалось распознать мнение.\n"
            "Попробуй еще раз или /cancel"
        )
        return WAITING_FOR_OPINION
    
    context.user_data['opinion_data'] = opinion_data
    film_data = context.user_data.get('film_data', {})
    
    # Показываем результат
    result = f"✅ <b>Готово!</b>\n\n"
    result += f"🎬 Фильм: {film_data.get('title', 'Неизвестен')}\n"
    result += f"⭐ Оценка: {opinion_data['rating']}/10\n"
    if opinion_data['hashtags']:
        result += f"🏷️ Настроение: {' '.join(opinion_data['hashtags'])}\n"
    if opinion_data['atmosphere_hashtags']:
        result += f"🌄 Атмосфера: {' '.join(opinion_data['atmosphere_hashtags'])}\n"
    
    await update.message.reply_text(result, parse_mode='HTML')
    
    # Очищаем
    context.user_data.clear()
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена"""
    context.user_data.clear()
    await update.message.reply_text("❌ Отменено. Нажми /start чтобы начать заново")
    return ConversationHandler.END

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

# ==================== ЗАПУСК ====================
print("🚀 Бот запущен!")
app.run_polling()
