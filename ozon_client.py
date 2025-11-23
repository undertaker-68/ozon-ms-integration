import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

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


def update_stocks(stocks: list):
    """
    Обновление остатков в Ozon.
    Ожидает список словарей вида:
    {
        "offer_id": "123",
        "stock": 10,
        "warehouse_id": 22254230484000
    }

    На стороне Ozon это мапится на /v2/products/stocks.
    """
    url = f"{OZON_API_URL}/v2/products/stocks"

    if not stocks:
        print("update_stocks: передан пустой список stocks, запрос к Ozon не отправляется.")
        return {"result": "no_stocks"}

    body = {
        "stocks": stocks
    }

    print("=== Тело запроса к Ozon /v2/products/stocks ===")
    print(body)
    print("=== /Тело запроса ===\n")

    r = requests.post(url, json=body, headers=HEADERS)

    print("=== Ответ Ozon /v2/products/stocks ===")
    print("HTTP status:", r.status_code)
    try:
        print("JSON:", r.json())
    except Exception:
        print("TEXT:", r.text)
    print("=== /Ответ Ozon ===\n")

    r.raise_for_status()
    return r.json()


BASE_URL = "https://api-seller.ozon.ru"


def get_fbs_postings(limit: int = 10) -> dict:
    """
    Получить список FBS-отправлений за последние 7 дней.

    Используем поля filter.since и filter.to,
    как в официальном примере документации.
    """

    url = f"{BASE_URL}/v3/posting/fbs/list"

    now_utc = datetime.now(timezone.utc)
    week_ago = now_utc - timedelta(days=7)

    since = week_ago.isoformat(timespec="seconds").replace("+00:00", "Z")
    to = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")

    body = {
        "dir": "ASC",
        "filter": {
            "since": since,
            "to": to,
            # статус можно пока не указывать, чтобы увидеть все:
            # "status": "awaiting_packaging",
            # по желанию потом добавим warehouse_id и др. фильтры
        },
        "limit": limit,
        "offset": 0,
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
