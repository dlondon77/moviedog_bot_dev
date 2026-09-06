# test_bot.py
import os
import configparser
import requests
from flask import Flask, request, jsonify

# ==================== КОНФИГ ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

config = configparser.ConfigParser(interpolation=None)
config.read(CONFIG_PATH)

# Функция для получения значения
def get_config_value(section, key):
    value = config[section][key]
    if value.startswith('%') and value.endswith('%'):
        env_var = value.strip('%')
        return os.environ.get(env_var, value)
    return value

TOKEN = get_config_value('CardBot', 'token')

print("🐕 ТЕСТОВЫЙ БОТ")
print("=" * 40)
print(f"Токен: {TOKEN[:10]}..." if TOKEN and not TOKEN.startswith('%') else f"❌ Токен: {TOKEN}")

if not TOKEN or TOKEN.startswith('%'):
    print("❌ Токен не найден! Проверьте переменные окружения.")
    exit(1)

# ==================== ПРОВЕРКА ТОКЕНА ====================
print("\n📌 Проверка токена...")
url = f"https://api.telegram.org/bot{TOKEN}/getMe"
try:
    response = requests.get(url, timeout=10)
    data = response.json()
    if data.get('ok'):
        print(f"✅ Бот подключен: @{data['result']['username']}")
    else:
        print(f"❌ Ошибка: {data}")
        exit(1)
except Exception as e:
    print(f"❌ Ошибка подключения: {e}")
    exit(1)

# ==================== FLASK ПРИЛОЖЕНИЕ ====================
app = Flask(__name__)

@app.route('/')
def index():
    return "🐕 Бот работает! Webhook: /webhook"

@app.route('/webhook', methods=['POST'])
def webhook():
    """Простейший обработчик"""
    try:
        data = request.get_json()
        print(f"📩 Получено: {data}")
        
        if 'message' in data:
            message = data['message']
            chat_id = message['chat']['id']
            text = message.get('text', '')
            user_id = message['from']['id']
            
            print(f"👤 User: {user_id}")
            print(f"💬 Text: {text}")
            
            # Отвечаем
            url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
            response = requests.post(url, json={
                "chat_id": chat_id,
                "text": f"🐕 Получил: {text}"
            })
            print(f"✅ Ответ отправлен")
        
        return jsonify({"status": "ok"})
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return jsonify({"status": "error"}), 500

@app.route('/setwebhook')
def set_webhook():
    """Установка webhook"""
    # Определяем URL автоматически
    host = request.host
    webhook_url = f"https://{host}/webhook"
    
    url = f"https://api.telegram.org/bot{TOKEN}/setWebhook"
    response = requests.post(url, json={"url": webhook_url})
    
    return jsonify({
        "webhook_url": webhook_url,
        "response": response.json()
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    print(f"\n🚀 Запуск на порту {port}")
    print(f"📌 Открой в браузере: https://ваш-домен.btohost.com/setwebhook")
    print("=" * 40)
    app.run(host='0.0.0.0', port=port)
