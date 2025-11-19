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
    Фильтр здесь минимальный, чтобы не усложнять.
    При необходимости параметры можно будет донастроить
    по сообщениям об ошибках от API Ozon.
    """
    url = f"{BASE_URL}/v3/posting/fbs/list"

    body = {
        "limit": limit,
        "offset": 0,
        "dir": "ASC",
        # минимальный фильтр; Ozon может потребовать уточнить since/to
        "filter": {},
        "with": {
            "analytics_data": True,
            "financial_data": True,
        },
    }

    r = requests.post(url, json=body, headers=HEADERS)
    r.raise_for_status()
    return r.json()


if __name__ == "__main__":
    print("=== Тест пустого запроса к /v2/products/stocks ===")
    test_resp = update_stocks([])
    print(test_resp)
