# bot.py
import os
import configparser
from telegram.ext import Application, CommandHandler

# ==================== ЧИТАЕМ КОНФИГ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

# Читаем конфиг без интерполяции (чтобы % не мешали)
config = configparser.ConfigParser(interpolation=None)
config.read(CONFIG_PATH)

# Получаем токен
token_raw = config['CardBot']['token']

# Если токен в формате %VAR%, берем из переменной окружения
if token_raw.startswith('%') and token_raw.endswith('%'):
    env_var = token_raw.strip('%')
    TOKEN = os.environ.get(env_var)
else:
    TOKEN = token_raw

# Проверяем
if not TOKEN:
    print("❌ Токен не найден!")
    print("Установите переменную окружения: CARD_BOT_TOKEN")
    print("Или напишите токен прямо в config.ini")
    exit(1)

print(f"✅ Токен: {TOKEN[:10]}...")

# ==================== БОТ ====================
async def start(update, context):
    await update.message.reply_text("🐕 Привет! Бот работает!")

app = Application.builder().token(TOKEN).build()
app.add_handler(CommandHandler("start", start))

print("🚀 Бот запущен!")
app.run_polling()
