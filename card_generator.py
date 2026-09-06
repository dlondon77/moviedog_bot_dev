# bot.py
from telegram.ext import Application, CommandHandler

TOKEN = "8820530223:AAEsG8fhJyuwl-VtxYGqWszbEDwYL1mpyPI"

async def start(update, context):
    await update.message.reply_text("🐕 Привет!")

app = Application.builder().token(TOKEN).build()
app.add_handler(CommandHandler("start", start))

print("🚀 Бот запущен!")
app.run_polling()
