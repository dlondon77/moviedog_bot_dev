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
    print("❌ Токен не найден!")
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

def send_message(chat_id, text):
    """Отправка сообщения"""
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)
    except Exception as e:
        print(f"❌ Ошибка отправки: {e}")

@app.route('/')
def index():
    return "🐕 Бот работает! Webhook: /webhook"

@app.route('/webhook', methods=['POST'])
def webhook():
    """Обработчик сообщений"""
    try:
        data = request.get_json()
        print(f"📩 Получено: {data}")
        
        if 'message' in data:
            msg = data['message']
            chat_id = msg['chat']['id']
            text = msg.get('text', '')
            user_id = msg['from']['id']
            
            print(f"👤 User: {user_id}")
            print(f"💬 Text: {text}")
            
            # Простой ответ
            if text == '/start':
                send_message(chat_id, "🐕 Привет! Я тестовый бот!")
            elif text == '/help':
                send_message(chat_id, "🐕 Команды: /start, /help, /test")
            elif text == '/test':
                send_message(chat_id, "✅ Бот работает!")
            else:
                send_message(chat_id, f"🐕 Ты написал: {text}")
        
        return jsonify({"status": "ok"})
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return jsonify({"status": "error"}), 500

@app.route('/setwebhook')
def set_webhook():
    """Установка webhook"""
    host = request.host
    webhook_url = f"https://{host}/webhook"
    
    url = f"https://api.telegram.org/bot{TOKEN}/setWebhook"
    response = requests.post(url, json={"url": webhook_url})
    
    return jsonify({
        "webhook_url": webhook_url,
        "response": response.json()
    })

@app.route('/deletewebhook')
def delete_webhook():
    """Удаление webhook"""
    url = f"https://api.telegram.org/bot{TOKEN}/deleteWebhook"
    response = requests.get(url)
    return jsonify(response.json())

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    print(f"\n🚀 Запуск на порту {port}")
    print(f"📌 Установи webhook: https://ваш-домен.btohost.com/setwebhook")
    print("=" * 40)
    app.run(host='0.0.0.0', port=port)
