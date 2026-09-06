# bot_step1.py
import os
import configparser
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Читаем конфиг
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

config = configparser.ConfigParser(interpolation=None)
config.read(CONFIG_PATH)

# Берем токен из конфига (НЕ через переменные окружения!)
TOKEN = config['CardBot']['token']

print("🐕 Запуск бота...")
print(f"Токен: {TOKEN[:10]}...")

# Создаем бота
app = Application.builder().token(TOKEN).build()

# Добавляем простую команду
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🐕 Бот работает!")

app.add_handler(CommandHandler("start", start))

print("🚀 Бот запущен!")
app.run_polling()
