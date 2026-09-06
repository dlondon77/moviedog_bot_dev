# bot.py
import os
import configparser
from telegram.ext import Application, CommandHandler

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

config = configparser.ConfigParser(interpolation=None)
config.read(CONFIG_PATH)

# Получаем токен из переменной окружения или конфига
def get_value(section, key):
    value = config[section][key]
    if value.startswith('%') and value.endswith('%'):
        env_var = value.strip('%')
        return os.environ.get(env_var)
    return value

TOKEN = get_value('CardBot', 'token')

if not TOKEN:
    print("❌ Токен не найден!")
    print("Установи переменную окружения: CARD_BOT_TOKEN")
    exit(1)

print(f"✅ Токен: {TOKEN[:10]}...")

async def start(update, context):
    await update.message.reply_text("🐕 Привет!")

app = Application.builder().token(TOKEN).build()
app.add_handler(CommandHandler("start", start))

print("🚀 Бот запущен!")
app.run_polling()
