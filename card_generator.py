# test_bot.py
import os
import configparser
import logging

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== КОНФИГУРАЦИЯ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

print("=" * 60)
print("🐕 ТЕСТОВЫЙ ЗАПУСК - ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ")
print("=" * 60)

# 1. Проверяем переменные окружения
print("\n📌 1. ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ:")
print("-" * 40)

env_vars = {
    'CARD_BOT_TOKEN': os.environ.get('CARD_BOT_TOKEN'),
    'OPENAI_API_KEY': os.environ.get('OPENAI_API_KEY'),
}

for var_name, var_value in env_vars.items():
    if var_value:
        # Показываем только первые 10 символов для безопасности
        display_value = var_value[:10] + "..." if len(var_value) > 10 else var_value
        print(f"✅ {var_name} = {display_value}")
    else:
        print(f"❌ {var_name} = НЕ НАЙДЕН!")

# 2. Проверяем чтение конфига
print("\n📌 2. ПРОВЕРКА ЧТЕНИЯ КОНФИГА:")
print("-" * 40)

try:
    # Читаем конфиг с отключенной интерполяцией
    config = configparser.ConfigParser(interpolation=None)
    config.read(CONFIG_PATH)
    
    print(f"✅ Конфиг загружен: {CONFIG_PATH}")
    print(f"   Секции: {', '.join(config.sections())}")
    
except Exception as e:
    print(f"❌ Ошибка загрузки конфига: {e}")
    config = None

# 3. Проверяем значения из конфига
print("\n📌 3. ПРОВЕРКА ЗНАЧЕНИЙ ИЗ КОНФИГА:")
print("-" * 40)

if config:
    # Проверяем секцию CardBot
    try:
        card_token_raw = config['CardBot']['token']
        print(f"📝 CardBot.token = {card_token_raw}")
        
        # Если значение начинается с %, пытаемся получить из переменной окружения
        if card_token_raw.startswith('%') and card_token_raw.endswith('%'):
            env_var_name = card_token_raw.strip('%')
            env_value = os.environ.get(env_var_name)
            if env_value:
                display_value = env_value[:10] + "..." if len(env_value) > 10 else env_value
                print(f"   → Из переменной {env_var_name}: {display_value}")
            else:
                print(f"   ❌ Переменная {env_var_name} не найдена!")
        else:
            print(f"   → Значение из конфига: {card_token_raw[:10]}...")
    except Exception as e:
        print(f"❌ Ошибка чтения CardBot.token: {e}")
    
    # Проверяем секцию OpenAI
    try:
        api_key_raw = config['OpenAI']['api_key']
        print(f"\n📝 OpenAI.api_key = {api_key_raw}")
        
        if api_key_raw.startswith('%') and api_key_raw.endswith('%'):
            env_var_name = api_key_raw.strip('%')
            env_value = os.environ.get(env_var_name)
            if env_value:
                display_value = env_value[:10] + "..." if len(env_value) > 10 else env_value
                print(f"   → Из переменной {env_var_name}: {display_value}")
            else:
                print(f"   ❌ Переменная {env_var_name} не найдена!")
        else:
            print(f"   → Значение из конфига: {api_key_raw[:10]}...")
    except Exception as e:
        print(f"❌ Ошибка чтения OpenAI.api_key: {e}")

# 4. Финальная проверка - готов ли бот к запуску
print("\n📌 4. ФИНАЛЬНАЯ ПРОВЕРКА ГОТОВНОСТИ:")
print("-" * 40)

# Получаем токен бота
card_token = os.environ.get('CARD_BOT_TOKEN')
if not card_token and config:
    token_raw = config['CardBot']['token']
    if token_raw.startswith('%') and token_raw.endswith('%'):
        env_var_name = token_raw.strip('%')
        card_token = os.environ.get(env_var_name)
    else:
        card_token = token_raw

# Получаем API ключ
api_key = os.environ.get('OPENAI_API_KEY')
if not api_key and config:
    api_key_raw = config['OpenAI']['api_key']
    if api_key_raw.startswith('%') and api_key_raw.endswith('%'):
        env_var_name = api_key_raw.strip('%')
        api_key = os.environ.get(env_var_name)
    else:
        api_key = api_key_raw

if card_token:
    print(f"✅ CARD_BOT_TOKEN: {card_token[:10]}... (готов к использованию)")
else:
    print("❌ CARD_BOT_TOKEN: НЕ НАЙДЕН!")

if api_key:
    print(f"✅ OPENAI_API_KEY: {api_key[:10]}... (готов к использованию)")
else:
    print("❌ OPENAI_API_KEY: НЕ НАЙДЕН!")

# 5. Вывод итогов
print("\n" + "=" * 60)
if card_token and api_key:
    print("✅ ВСЕ ПЕРЕМЕННЫЕ НАЙДЕНЫ! БОТ ГОТОВ К ЗАПУСКУ!")
    print("\nЧтобы запустить бота, выполните:")
    print("  python bot_card.py")
else:
    print("❌ НЕКОТОРЫЕ ПЕРЕМЕННЫЕ ОТСУТСТВУЮТ!")
    print("\nЧтобы установить переменные окружения:")
    print("\n  В Linux/Mac:")
    print("  export CARD_BOT_TOKEN=8820530223:AAEsG8fhJyuwl-VtxYGqWszbEDwYL1mpyPI")
    print("  export OPENAI_API_KEY=sk-1be9e78c0cd347d4bdda68f8b54e0992")
    print("  python bot_card.py")
    print("\n  В Windows (cmd):")
    print("  set CARD_BOT_TOKEN=8820530223:AAEsG8fhJyuwl-VtxYGqWszbEDwYL1mpyPI")
    print("  set OPENAI_API_KEY=sk-1be9e78c0cd347d4bdda68f8b54e0992")
    print("  python bot_card.py")
print("=" * 60)
