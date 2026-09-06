# bot_card.py
import os
import logging
import configparser
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    ConversationHandler,
    CallbackQueryHandler,
)

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Загрузка конфигурации
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

TELEGRAM_TOKEN = config['Telegram']['token']

# Состояния для ConversationHandler
WAITING_FOR_FILM = 1
WAITING_FOR_OPINION = 2
WAITING_FOR_FRAMES = 3

# Импортируем генератор карточек
from card_generator import generate_cards, save_template


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    await update.message.reply_text(
        "🐕 Привет! Я КиноИщейка — твой помощник для создания обложек для кино-обзоров!\n\n"
        "Чтобы создать 5 карточек, мне нужно:\n"
        "1️⃣ Текст о фильме (название, страна, режиссёр, актёры)\n"
        "2️⃣ Текст мнения (оценка, настроение, атмосфера, сам отзыв)\n"
        "3️⃣ 5 кадров из фильма (по одному на каждый слайд)\n\n"
        "Давай начнём! Отправь мне текст о фильме в формате:\n\n"
        "Название фильма (2024)\n"
        "Страна: Россия\n"
        "Режиссер: Иван Иванов\n"
        "Актеры: Петр Петров, Анна Сидорова\n\n"
        "Или отправь /cancel чтобы отменить"
    )
    return WAITING_FOR_FILM


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена операции"""
    await update.message.reply_text("❌ Операция отменена. Начни заново с /start")
    return ConversationHandler.END


async def get_film_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получает текст о фильме"""
    context.user_data['film_text'] = update.message.text
    await update.message.reply_text(
        "✅ Текст о фильме получен!\n\n"
        "Теперь отправь текст с мнением о фильме в формате:\n\n"
        "Твой отзыв...\n"
        "Оценка: 8\n"
        "Настроение: #классно #интересно\n"
        "Атмосфера: #тёмная #атмосферная\n\n"
        "Или отправь /cancel чтобы отменить"
    )
    return WAITING_FOR_OPINION


async def get_opinion_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получает текст мнения"""
    context.user_data['opinion_text'] = update.message.text
    await update.message.reply_text(
        "✅ Текст мнения получен!\n\n"
        "Теперь отправь 5 кадров из фильма (изображения).\n"
        "Отправляй их по одному, я буду сохранять их в порядке получения.\n\n"
        "Отправь 5 изображений (или /cancel чтобы отменить)"
    )
    return WAITING_FOR_FRAMES


async def get_frames(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получает кадры из фильма"""
    if 'frames' not in context.user_data:
        context.user_data['frames'] = []
    
    photo = update.message.photo[-1]
    file = await photo.get_file()
    
    frame_num = len(context.user_data['frames']) + 1
    frame_path = f"frames/frame_{frame_num}_{int(update.message.date.timestamp())}.jpg"
    await file.download_to_drive(frame_path)
    context.user_data['frames'].append(frame_path)
    
    await update.message.reply_text(f"📸 Кадр {frame_num}/5 получен!")
    
    if len(context.user_data['frames']) == 5:
        await update.message.reply_text("🎬 Все 5 кадров получены! Начинаю генерацию карточек...")
        
        # Генерируем карточки
        output_paths = generate_cards(
            film_text=context.user_data['film_text'],
            opinion_text=context.user_data['opinion_text'],
            frame_paths=context.user_data['frames']
        )
        
        if not output_paths:
            await update.message.reply_text(
                "❌ Произошла ошибка при генерации карточек. Попробуй еще раз."
            )
            return ConversationHandler.END
        
        # Отправляем готовые карточки
        await update.message.reply_text("📤 Отправляю готовые карточки...")
        
        slide_titles = [
            "О чём лай?",
            "Какая атмосфера?",
            "Какая игра?",
            "Что зарыто?",
            "Какой вердикт?"
        ]
        
        for i, path in enumerate(output_paths):
            if os.path.exists(path):
                with open(path, "rb") as f:
                    await update.message.reply_photo(f, caption=f"Слайд {i+1}: {slide_titles[i]}")
                os.remove(path)  # Удаляем временный файл
        
        # Очищаем временные файлы
        for path in context.user_data['frames']:
            if os.path.exists(path):
                os.remove(path)
        
        await update.message.reply_text(
            "🎉 Готово! Все 5 карточек созданы!\n"
            "Хочешь создать ещё один обзор? Отправь /start"
        )
        
        context.user_data.clear()
        return ConversationHandler.END
    
    return WAITING_FOR_FRAMES


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help"""
    await update.message.reply_text(
        "🐕 КиноИщейка - помощник для создания обложек для кино-обзоров\n\n"
        "Команды:\n"
        "/start - Начать создание нового обзора\n"
        "/help - Показать это сообщение\n"
        "/cancel - Отменить текущую операцию\n\n"
        "Как это работает:\n"
        "1. Отправь текст о фильме\n"
        "2. Отправь текст мнения с оценкой и хэштегами\n"
        "3. Отправь 5 кадров из фильма\n"
        "4. Бот создаст 5 красивых карточек-обложек!"
    )


def main():
    """Запуск бота"""
    # Сохраняем шаблон при запуске
    save_template()
    
    # Создаем приложение
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Создаем ConversationHandler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            WAITING_FOR_FILM: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_film_text)],
            WAITING_FOR_OPINION: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_opinion_text)],
            WAITING_FOR_FRAMES: [MessageHandler(filters.PHOTO, get_frames)],
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )
    
    application.add_handler(conv_handler)
    application.add_handler(CommandHandler('help', help_command))
    
    # Запускаем бота
    print("🐕 КиноИщейка бот запущен!")
    print(f"Используется конфиг: {CONFIG_PATH}")
    print("Для остановки нажмите Ctrl+C")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
