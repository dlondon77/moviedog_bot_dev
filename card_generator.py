# card_bot.py
import os
import configparser
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ==================== ОТКЛЮЧАЕМ ПРОКСИ ====================
for env_var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(env_var, None)

# ==================== КОНФИГУРАЦИЯ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

config = configparser.ConfigParser(interpolation=None)
config.read(CONFIG_PATH)

def get_config_value(section, key):
    value = config[section][key]
    if value.startswith('%') and value.endswith('%'):
        env_var = value.strip('%')
        return os.environ.get(env_var, value)
    return value

TOKEN = get_config_value('CardBot', 'token')
ALLOWED_USER_ID = 397469639

print("🐕 Самый простой бот")
print("=" * 40)
print(f"Токен: {TOKEN[:10]}..." if TOKEN else "❌ Токен не найден")
print("=" * 40)

# ==================== КОМАНДЫ ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    user_id = update.message.from_user.id
    
    if user_id != ALLOWED_USER_ID:
        await update.message.reply_text("❌ Доступ запрещен")
        return
    
    await update.message.reply_text("🐕 Привет! Это самый простой бот!")

# ==================== MAIN ====================

def main():
    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    
    print("🚀 Бот запущен!")
    application.run_polling()

if __name__ == "__main__":
    main()
