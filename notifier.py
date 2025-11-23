import os
import requests

TG_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")  # может быть ID канала/группы

def send_telegram_message(text: str):
    """
    Отправляет сообщение в Telegram-группу/чат.
    TG_TOKEN и TG_CHAT_ID берутся из .env
    """
    if not TG_TOKEN or not TG_CHAT_ID:
        print("Telegram не настроен: нет TG_BOT_TOKEN или TG_CHAT_ID")
        return False

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }

    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            print("Ошибка Telegram:", r.text)
            return False
    except Exception as e:
        print("Ошибка отправки Telegram:", e)
        return False

    return True
