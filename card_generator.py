# test_bot.py
import os
import httpx
from telegram import Update, InputMediaPhoto
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
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
    print("❌ Ошибка: установите CARD_BOT_TOKEN")
    exit(1)

print(f"✅ Токен: {TOKEN[:10]}...")

# ==================== ПУТИ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRAMES_DIR = os.path.join(BASE_DIR, 'frames')
os.makedirs(FRAMES_DIR, exist_ok=True)

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

# ==================== КОМАНДЫ ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🐕 <b>Тестовый бот</b>\n\n"
        "Отправь 5 фото одним сообщением.\n"
        "Я сохраню их и покажу, что получил.",
        parse_mode='HTML'
    )

async def handle_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает сообщение с фото"""
    
    # Проверяем, есть ли фото
    if not update.message.photo:
        await update.message.reply_text("❌ Отправь фото!")
        return
    
    photos = update.message.photo
    count = len(photos)
    
    await update.message.reply_text(f"📸 Получено {count} фото!")
    
    # Сохраняем каждое фото
    saved_paths = []
    for i, photo in enumerate(photos):
        file = await photo.get_file()
        file_path = os.path.join(FRAMES_DIR, f"test_frame_{i+1}.jpg")
        await file.download_to_drive(file_path)
        saved_paths.append(file_path)
        await update.message.reply_text(f"✅ Фото {i+1} сохранено: {file_path}")
    
    # Отправляем результат
    await update.message.reply_text(
        f"🎉 Сохранено {len(saved_paths)} файлов:\n" + 
        "\n".join([f"📸 {p}" for p in saved_paths])
    )
    
    # Отправляем обратно первые 3 фото (как пример)
    if len(saved_paths) >= 3:
        media_group = []
        for i in range(min(3, len(saved_paths))):
            with open(saved_paths[i], 'rb') as f:
                media_group.append(
                    InputMediaPhoto(
                        media=f,
                        caption=f"Фото {i+1}" if i == 0 else None
                    )
                )
        
        await update.message.reply_media_group(media_group)
        await update.message.reply_text("📸 Вот твои первые 3 фото (тест)")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "❌ Отправь фото!\n"
        "Нажми /start для инструкции"
    )

# ==================== ОБРАБОТЧИКИ ====================
app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.PHOTO, handle_photos))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

# ==================== ЗАПУСК ====================
print("🚀 Тестовый бот запущен!")
print("Отправь 5 фото одним сообщением для теста")
app.run_polling()
