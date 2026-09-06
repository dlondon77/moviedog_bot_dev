# card_bot.py
import os
import configparser
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler

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

# ==================== ЛОГИРОВАНИЕ ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

print("🐕 КиноИщейка - генератор карточек")
print("=" * 40)
print(f"Токен: {TOKEN[:10]}..." if TOKEN else "❌ Токен не найден")
print("=" * 40)

# ==================== КОМАНДЫ ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start - показывает приветствие с кнопкой"""
    user_id = update.message.from_user.id
    
    if user_id != ALLOWED_USER_ID:
        await update.message.reply_text("❌ Доступ запрещен")
        return
    
    # Создаем кнопку
    keyboard = [
        [InlineKeyboardButton("📝 Создать карточки", callback_data="create_cards")],
        [InlineKeyboardButton("❓ Помощь", callback_data="help")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🐕 <b>КиноИщейка - генератор карточек</b>\n\n"
        "Я помогу тебе разбить мнение о фильме на 5 слайдов.\n\n"
        "Нажми кнопку ниже чтобы начать:",
        parse_mode='HTML',
        reply_markup=reply_markup
    )

# ==================== ОБРАБОТКА КНОПОК ====================

async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий на кнопки"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    if user_id != ALLOWED_USER_ID:
        await query.message.reply_text("❌ Доступ запрещен")
        return
    
    data = query.data
    
    if data == "create_cards":
        await query.edit_message_text(
            "📝 <b>Шаг 1 из 2: Отправь текст о фильме</b>\n\n"
            "Пример:\n"
            "<code>🎬 Как избежать наказания за убийство (2014)\n"
            "🌍 Страна: США\n"
            "🎥 Режиссер: Лора Иннес, Майкл Смит\n"
            "👥 Актеры: Чарли Уэбер, Лиза Вейл</code>\n\n"
            "Или нажми /cancel для отмены",
            parse_mode='HTML'
        )
    
    elif data == "help":
        await query.edit_message_text(
            "🐕 <b>Помощь</b>\n\n"
            "1. Нажми кнопку 'Создать карточки'\n"
            "2. Отправь текст о фильме\n"
            "3. Отправь текст мнения\n"
            "4. Получи 5 готовых слайдов!",
            parse_mode='HTML'
        )

# ==================== MAIN ====================

def main():
    """Запуск бота"""
    # Создаем приложение
    application = Application.builder().token(TOKEN).build()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(handle_button))
    
    # Запускаем бота
    print("🚀 Бот запущен!")
    print(f"Разрешенный пользователь: {ALLOWED_USER_ID}")
    print("Для остановки нажмите Ctrl+C")
    print("=" * 40)
    
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
