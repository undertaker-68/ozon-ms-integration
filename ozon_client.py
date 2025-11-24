import os
import json
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

try:
    from notifier import send_telegram_message
except ImportError:
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
    Использует /v3/product/info/list.

    Так как в ответе нет поля "state", строим своё:
      - ARCHIVED, если is_archived или is_autoarchived = True
      - ACTIVE  во всех остальных случаях
    """
    if not offer_ids:
        return {}

    url = f"{OZON_API_URL}/v3/product/info/list"

    BATCH_SIZE = 1000
    result = {}

    for i in range(0, len(offer_ids), BATCH_SIZE):
        batch = offer_ids[i:i + BATCH_SIZE]

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

        r.raise_for_status()

        items = data.get("items") or data.get("result") or []

        for item in items:
            oid = item.get("offer_id")
            if not oid:
                continue

            is_archived = bool(item.get("is_archived")) or bool(item.get("is_autoarchived"))
            state = "ARCHIVED" if is_archived else "ACTIVE"
            result[oid] = state

    # те offer_id, которые вообще не вернулись из Ozon — считаем неизвестными
    for oid in offer_ids:
        if oid not in result:
            result[oid] = None

    return result


def update_stocks(stocks: list) -> dict:
    """
    Обновление остатков в Ozon.

    Ожидает список словарей вида:
    {
        "offer_id": "ART123",
        "stock": 10,
        "warehouse_id": 22254230484000
    }
    """
    if not stocks:
        print("update_stocks: передан пустой список stocks, запрос к Ozon не отправляется.")
        return {"result": []}

    url = f"{OZON_API_URL}/v2/products/stocks"
    body = {"stocks": stocks}
