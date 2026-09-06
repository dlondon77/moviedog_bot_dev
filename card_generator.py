# bot.py
import configparser
from telegram.ext import Application, CommandHandler

config = configparser.ConfigParser()
config.read('config.ini')

TOKEN = config['CardBot']['token']

async def start(update, context):
    await update.message.reply_text("🐕 Привет!")

app = Application.builder().token(TOKEN).build()
app.add_handler(CommandHandler("start", start))

print("🚀 Бот запущен!")
app.run_polling()
