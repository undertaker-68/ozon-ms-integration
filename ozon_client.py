import os
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

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

BASE_URL = "https://api-seller.ozon.ru"


def update_stocks(stocks: list) -> dict:
    """
    Обновить остатки товаров на складах Ozon.

    stocks = [
      {"offer_id": "ART123", "stock": 10, "warehouse_id": 22254230484000},
      ...
    ]
    """
    url = f"{BASE_URL}/v2/products/stocks"
    body = {"stocks": stocks}
    r = requests.post(url, json=body, headers=HEADERS)
    r.raise_for_status()
    return r.json()


def get_fbs_postings(limit: int = 10) -> dict:
    """
    Получить список FBS-отправлений (режим отладки).

    ВАЖНО: Ozon требует поля processed_at_from / processed_at_to
    прямо в filter, а не вложенный объект.
    Берём отправления за последние 7 дней.
    """
    url = f"{BASE_URL}/v3/posting/fbs/list"

    now_utc = datetime.now(timezone.utc)
    week_ago = now_utc - timedelta(days=7)

    processed_at_from = week_ago.isoformat(timespec="seconds").replace("+00:00", "Z")
    processed_at_to = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")

    body = {
        "limit": limit,
        "offset": 0,
        "dir": "ASC",
        "filter": {
            # ВАЖНО: плоские поля, а не вложенный объект
            "processed_at_from": processed_at_from,
            "processed_at_to": processed_at_to,
            # статус пока не указываем, потом можно будет сузить
            # "status": "delivered",
        },
        "with": {
            "analytics_data": True,
            "financial_data": True,
        },
    }

    print("=== Тело запроса к Ozon /v3/posting/fbs/list ===")
    print(body)
    print("=== /Тело запроса ===\n")

    r = requests.post(url, json=body, headers=HEADERS)

    print("=== Ответ Ozon /v3/posting/fbs/list ===")
    print("HTTP status:", r.status_code)
    print("Response text:")
    print(r.text)
    print("=== /Ответ Ozon ===\n")

    r.raise_for_status()
    return r.json()


if __name__ == "__main__":
    # небольшой тест: пустое обновление остатков
    print("=== Тест пустого запроса к /v2/products/stocks ===")
    test_resp = update_stocks([])
    print(test_resp)
