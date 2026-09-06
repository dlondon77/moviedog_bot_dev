# bot.py
import os
import re
import base64
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

if not TOKEN or not DEEPSEEK_API_KEY:
    print("❌ Ошибка: установите CARD_BOT_TOKEN и OPENAI_API_KEY")
    exit(1)

print(f"✅ Токен: {TOKEN[:10]}...")
print(f"✅ API Key: {DEEPSEEK_API_KEY[:10]}...")

# ==================== ПУТИ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, 'templates')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

os.makedirs(TEMPLATES_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==================== СОСТОЯНИЯ ====================
WAITING_FOR_IMAGE = 1

DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"

# ==================== ФУНКЦИИ ====================

def get_template():
    template_path = os.path.join(TEMPLATES_DIR, 'opinion.html')
    with open(template_path, 'r', encoding='utf-8') as f:
        return f.read()

def generate_card_html(frame_path, text, is_first=True):
    """Генерирует HTML для одной карточки"""
    html = get_template()
    
    with open(frame_path, 'rb') as f:
        img_data = base64.b64encode(f.read()).decode('utf-8')
    
    html = html.replace("{{FRAME}}", f"data:image/jpeg;base64,{img_data}")
    html = html.replace("{{SLIDE_TITLE}}", "Мнение КиноИщейки")
    html = html.replace("{{SLIDE_TEXT}}", text)
    html = html.replace("{{RUBRIC}}", "КиноИщейка")
    html = html.replace("{{ARROW_VISIBLE}}", "" if is_first else "hidden")
    html = html.replace("{{TITLE}}", "Ваш фильм")
    html = html.replace("{{COUNTRY}}", "")
    html = html.replace("{{DIRECTOR}}", "")
    html = html.replace("{{ACTORS}}", "")
    html = html.replace("{{FILM_INFO_VISIBLE}}", "")
    html = html.replace("{{RATING}}", "")
    html = html.replace("{{HASHTAGS}}", "")
    
    return html

def html_to_image(html_content, output_path):
    """Конвертирует HTML в изображение через playwright"""
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
        print(f"Ошибка конвертации: {e}")
        return False

# ==================== КОМАНДЫ ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📸 Отправить картинку", callback_data="send_image")],
        [InlineKeyboardButton("📝 Отправить текст", callback_data="send_text")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🐕 <b>КиноИщейка - генератор карточки</b>\n\n"
        "Выбери действие:",
        parse_mode='HTML',
        reply_markup=reply_markup
    )

async def send_image_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text(
        "📸 Отправь изображение (кадр из фильма)\n"
        "Или /cancel для отмены"
    )
    return WAITING_FOR_IMAGE

async def send_text_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text(
        "📝 Отправь текст (мнение о фильме)\n"
        "Или /cancel для отмены"
    )
    return WAITING_FOR_IMAGE

async def handle_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получаем картинку и текст, генерируем карточку"""
    photo = update.message.photo[-1]
    file = await photo.get_file()
    
    # Сохраняем картинку
    frame_path = os.path.join(OUTPUT_DIR, "frame.jpg")
    await file.download_to_drive(frame_path)
    
    # Ждем текст
    await update.message.reply_text(
        "📝 Теперь отправь текст (мнение о фильме)\n"
        "Или /cancel для отмены"
    )
    
    context.user_data['frame_path'] = frame_path
    return WAITING_FOR_IMAGE

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получаем текст и генерируем карточку"""
    text = update.message.text
    frame_path = context.user_data.get('frame_path')
    
    if not frame_path:
        await update.message.reply_text("❌ Сначала отправь картинку!")
        return WAITING_FOR_IMAGE
    
    await update.message.reply_text("🔄 Генерирую карточку...")
    
    try:
        # Генерируем HTML
        html = generate_card_html(frame_path, text)
        
        # Сохраняем в изображение
        output_path = os.path.join(OUTPUT_DIR, "card.png")
        success = html_to_image(html, output_path)
        
        if success:
            with open(output_path, 'rb') as f:
                await update.message.reply_photo(f, caption="🐕 Готово!")
            os.remove(output_path)
        else:
            await update.message.reply_text("❌ Ошибка при генерации карточки")
        
        # Очищаем
        if os.path.exists(frame_path):
            os.remove(frame_path)
        context.user_data.clear()
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")
    
    return ConversationHandler.END

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
    entry_points=[
        CallbackQueryHandler(send_image_prompt, pattern="^send_image$"),
        CallbackQueryHandler(send_text_prompt, pattern="^send_text$"),
    ],
    states={
        WAITING_FOR_IMAGE: [
            MessageHandler(filters.PHOTO, handle_image),
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text),
        ],
    },
    fallbacks=[CommandHandler("cancel", cancel)]
)
app.add_handler(conv_handler)

# ==================== ЗАПУСК ====================
print("🚀 Бот запущен!")
print("=" * 40)
app.run_polling()
