# bot.py
import os
import httpx
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.request import HTTPXRequest

# ==================== ПАТЧ ДЛЯ ПРОКСИ ====================
# Сохраняем оригинальный __init__
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

# ==================== БОТ ====================
# Создаем кастомный клиент
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

# Создаем приложение с кастомным клиентом
app = Application.builder().token(TOKEN).request(custom_request).build()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🐕 Привет! Бот работает!")

app.add_handler(CommandHandler("start", start))

print("🚀 Бот запущен!")
app.run_polling()
