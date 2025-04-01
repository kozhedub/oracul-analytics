import os
import requests
from dotenv import load_dotenv

print("📄 Запущен скрипт: <название>")
logging.info("📄 Запущен скрипт: <название>")


load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram_message(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Отсутствует TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID в .env")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }

    try:
        response = requests.post(url, data=payload)
        if response.status_code != 200:
            print(f"⚠️ Ошибка отправки Telegram: {response.status_code} — {response.text}")
    except Exception as e:
        print(f"❌ Telegram Error: {e}")

def send_telegram_photo(photo_path, caption=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    with open(photo_path, "rb") as photo:
        data = {"chat_id": CHAT_ID, "caption": caption or ""}
        files = {"photo": photo}
        response = requests.post(url, data=data, files=files)
        if not response.ok:
            print(f"❌ Ошибка отправки фото: {response.text}")