import os
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
    Получить список FBS-отправлений.
    Сейчас работаем в режиме отладки: печатаем полный ответ Ozon,
    даже если он вернул 400/422 и т.п., чтобы понять, чего ему не хватает.
    """
    url = f"{BASE_URL}/v3/posting/fbs/list"

    # Минимальный фильтр. Ozon, скорее всего, попросит since/to или другие поля.
    body = {
        "limit": limit,
        "offset": 0,
        "dir": "ASC",
        "filter": {
            # начальная дата; при необходимости потом поменяем
            "since": "2025-01-01T00:00:00Z"
            # "to": "2025-12-31T23:59:59Z",  # можно будет добавить позже
            # статусы пока не указываем, чтобы не ловить конфликтов
        },
        "with": {
            "analytics_data": True,
            "financial_data": True,
        },
    }

    r = requests.post(url, json=body, headers=HEADERS)

    # ВАЖНО: печатаем тело ответа всегда, даже если статус не 200
    print("=== Ответ Ozon /v3/posting/fbs/list ===")
    print("HTTP status:", r.status_code)
    print("Response text:")
    print(r.text)
    print("=== /Ответ Ozon ===\n")

    # Потом всё равно бросаем исключение, если статус не ок — чтобы sync_orders знал, что что-то не так
    r.raise_for_status()

    return r.json()


if __name__ == "__main__":
    print("=== Тест пустого запроса к /v2/products/stocks ===")
    test_resp = update_stocks([])
    print(test_resp)
