# card_bot.py
import os
import configparser
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ==================== КОНФИГ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

config = configparser.ConfigParser(interpolation=None)
config.read(CONFIG_PATH)

TOKEN = config['CardBot']['token']
ALLOWED_USER_ID = 397469639

# ==================== КОМАНДЫ ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ALLOWED_USER_ID:
        await update.message.reply_text("❌ Доступ запрещен")
        return
    await update.message.reply_text("🐕 Привет! Бот работает!")

# ==================== ЗАПУСК ====================

def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    print("🚀 Бот запущен!")
    app.run_polling()

if __name__ == "__main__":
    main()
