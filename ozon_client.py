import os
import json
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

try:
    # наш модуль для уведомлений
    from notifier import send_telegram_message
except ImportError:
    # запасной вариант, чтобы файл не падал, если notifier нет
    def send_telegram_message(text: str) -> bool:
        print("Telegram notifier не доступен:", text)
        return False


load_dotenv()

CLIENT_ID = os.getenv("OZON_CLIENT_ID")
API_KEY = os.getenv("OZON_API_KEY")

if not CLIENT_ID or not API_KEY:
    raise RuntimeError("Не заданы OZON_CLIENT_ID / OZON_API_KEY в .env")

HEADERS = {
    "Client-Id": CLIENT_ID,
    "Api-Key": API_KEY,
    "Content-Type": "application/json",
}

OZON_API_URL = "https://api-seller.ozon.ru"


def get_products_state_by_offer_ids(offer_ids):
    """
    Возвращает словарь {offer_id: state} для переданных offer_id.
    state, как правило: ACTIVE, ARCHIVED, DISABLED и т.п.

    Использует эндпоинт /v3/product/info/list.
    """
    if not offer_ids:
        return {}

    url = f"{OZON_API_URL}/v3/product/info/list"

    # Ozon обычно позволяет до 1000 offer_id за раз, на всякий случай батчим
    BATCH_SIZE = 1000
    result = {}

    for i in range(0, len(offer_ids), BATCH_SIZE):
        batch = offer_ids[i: i + BATCH_SIZE]

        body = {
            "offer_id": batch,
            "product_id": [],
            "sku": [],
        }

        print("=== Тело запроса к Ozon /v3/product/info/list ===")
        print(body)
        print("=== /Тело запроса ===\n")

        r = requests.post(url, json=body, headers=HEADERS, timeout=30)
        print("=== Ответ Ozon /v3/product/info/list ===")
        print("HTTP status:", r.status_code)
        try:
            data = r.json()
            print("JSON (фрагмент):", str(data)[:500])
        except Exception:
            data = {}
            print("TEXT:", r.text[:500])
        print("=== /Ответ Ozon ===\n")

        # если эндпоинт не доступен или ключи неверные — пусть падает явно
        r.raise_for_status()

        # подстрахуемся на оба варианта структуры ответа
        items = data.get("result") or data.get("items") or []

        for item in items:
            oid = item.get("offer_id")
            state = item.get("state")  # обычно тут ARCHIVED / ACTIVE / и т.п.
            if oid:
                result[oid] = state

    return result


def update_stocks(stocks: list) -> dict:
    """
    Обновление остатков в Ozon.

    Ожидает список словарей вида:
    {
        "offer_id": "ART_
