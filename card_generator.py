# bot_card.py
import os
import configparser
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# ==================== ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ====================
print("🐕 ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ")
print("=" * 40)

card_token = os.environ.get('CARD_BOT_TOKEN')
api_key = os.environ.get('OPENAI_API_KEY')

if card_token:
    print(f"✅ CARD_BOT_TOKEN = {card_token[:10]}...")
else:
    print("❌ CARD_BOT_TOKEN не найден")
    exit(1)

if api_key:
    print(f"✅ OPENAI_API_KEY = {api_key[:10]}...")
else:
    print("❌ OPENAI_API_KEY не найден")
    exit(1)

print("=" * 40)
print("✅ Все переменные найдены! Запускаю бота...")
print("=" * 40)

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== КОНФИГУРАЦИЯ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

# Загрузка конфига
config = configparser.ConfigParser(interpolation=None)
config.read(CONFIG_PATH)

# Только для этого user_id
ALLOWED_USER_ID = 397469639

# ==================== КОМАНДЫ ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверяем user_id и выводим переменные"""
    user_id = update.message.from_user.id
    
    if user_id != ALLOWED_USER_ID:
        await update.message.reply_text(
            "❌ Извините, этот бот только для тестирования.\n"
            "Доступ запрещен."
        )
        return
    
    # Получаем переменные окружения
    card_token = os.environ.get('CARD_BOT_TOKEN', 'НЕ НАЙДЕН')
    api_key = os.environ.get('OPENAI_API_KEY', 'НЕ НАЙДЕН')
    
    # Формируем сообщение
    message = (
        "🐕 <b>Переменные окружения:</b>\n\n"
        f"📌 CARD_BOT_TOKEN: <code>{card_token}</code>\n"
        f"📌 OPENAI_API_KEY: <code>{api_key}</code>\n\n"
        "✅ Бот работает!"
    )
    
    await update.message.reply_text(message, parse_mode='HTML')


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просто эхо для проверки"""
    user_id = update.message.from_user.id
    
    if user_id != ALLOWED_USER_ID:
        await update.message.reply_text("❌ Доступ запрещен")
        return
    
    await update.message.reply_text(f"🐕 Вы написали: {update.message.text}")


# ==================== MAIN ====================

def main():
    # Создаем приложение
    application = Application.builder().token(card_token).build()
    
    # Добавляем команды
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))
    
    # Запускаем бота
    print("🐕 Бот запущен!")
    print(f"Доступен только для user_id: {ALLOWED_USER_ID}")
    print("Для остановки нажмите Ctrl+C")
    
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
