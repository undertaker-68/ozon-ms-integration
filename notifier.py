import os
import requests
from dotenv import load_dotenv

# Явно загружаем .env из текущей директории проекта
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(BASE_DIR, ".env")
load_dotenv(env_path)

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")


def send_telegram_message(text: str) -> bool:
    """
    Отправляет сообщение в Telegram-группу/чат.
    """
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print(f"Telegram не настроен: нет TG_BOT_TOKEN или TG_CHAT_ID "
              f"(TG_BOT_TOKEN={TG_BOT_TOKEN}, TG_CHAT_ID={TG_CHAT_ID})")
        return False

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }

    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            print("Ошибка Telegram:", r.status_code, r.text)
            return False
    except Exception as e:
        print("Ошибка отправки Telegram:", e)
        return False

    return True


def send_telegram_document(file_path: str, caption: str = "") -> bool:
    """
    Отправляет файл (документ) в Telegram-чат.
    file_path — путь к локальному файлу.
    caption — подпись к документу (необязательно).
    """
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print(f"Telegram не настроен: нет TG_BOT_TOKEN или TG_CHAT_ID "
              f"(TG_BOT_TOKEN={TG_BOT_TOKEN}, TG_CHAT_ID={TG_CHAT_ID})")
        return False

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument"

    try:
        with open(file_path, "rb") as f:
            files = {"document": f}
            data = {
                "chat_id": TG_CHAT_ID,
                "caption": caption,
            }
            r = requests.post(url, data=data, files=files, timeout=30)
        if r.status_code != 200:
            print("Ошибка Telegram (sendDocument):", r.status_code, r.text)
            return False
    except Exception as e:
        print("Ошибка отправки файла в Telegram:", e)
        return False

    return True
