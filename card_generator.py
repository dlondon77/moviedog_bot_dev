# bot.py
from telegram.ext import Application, CommandHandler
import os

TOKEN = os.environ.get('CARD_BOT_TOKEN')

if not TOKEN:
    print("❌ Ошибка: переменная CARD_BOT_TOKEN не установлена")
    exit(1)

async def start(update, context):
    await update.message.reply_text("🐕 Привет!")

app = Application.builder().token(TOKEN).build()
app.add_handler(CommandHandler("start", start))

print("🚀 Бот запущен!")
app.run_polling()
