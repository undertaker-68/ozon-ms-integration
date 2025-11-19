import os
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("OZON_CLIENT_ID")
API_KEY = os.getenv("OZON_API_KEY")
WAREHOUSE_ID = os.getenv("OZON_WAREHOUSE_ID")  # добавим в .env чуть позже

if not CLIENT_ID or not API_KEY:
    raise RuntimeError("Не заданы OZON_CLIENT_ID / OZON_API_KEY в .env")

HEADERS = {
    "Client-Id": CLIENT_ID,
    "Api-Key": API_KEY,
    "Content-Type": "application/json",
}

BASE_URL = "https://api-seller.ozon.ru"


def update_stocks(stocks: list):
    """
    Обновить остатки товаров на складах.
    stocks = [
      {"offer_id": "ART123", "stock": 10, "warehouse_id": 123456789},
      ...
    ]
    """
    url = f"{BASE_URL}/v2/products/stocks"
    body = {"stocks": stocks}
    r = requests.post(url, json=body, headers=HEADERS)
    r.raise_for_status()
    return r.json()


def test_simple_request():
    """
    Простой тестовый запрос к какому-нибудь методу,
    чтобы проверить, что ключи Ozon рабочие.
    Возьмем products/stocks, но без реального изменения (передадим пустой список).
    """
    url = f"{BASE_URL}/v2/products/stocks"
    body = {"stocks": []}
    r = requests.post(url, json=body, headers=HEADERS)
    r.raise_for_status()
    return r.json()


if __name__ == "__main__":
    # Тест: просто проверим, что запрос работает (должен вернуть пустой результат)
    print("=== Тест Ozon API /v2/products/stocks с пустым списком ===")
    resp = test_simple_request()
    print(resp)

