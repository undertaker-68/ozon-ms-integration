import base64
import os
import requests
from dotenv import load_dotenv

load_dotenv()

MS_LOGIN = os.getenv("MS_LOGIN")
MS_PASSWORD = os.getenv("MS_PASSWORD")

if not MS_LOGIN or not MS_PASSWORD:
    raise RuntimeError("Не заданы MS_LOGIN / MS_PASSWORD в .env")

# Basic auth в заголовке Authorization
AUTH = base64.b64encode(f"{MS_LOGIN}:{MS_PASSWORD}".encode("utf-8")).decode("utf-8")

HEADERS = {
    "Authorization": f"Basic {AUTH}",
    "Accept": "application/json;charset=utf-8",
    "Accept-Encoding": "gzip",
}

BASE_URL = "https://api.moysklad.ru/api/remap/1.2"


def get_products(limit: int = 10, offset: int = 0) -> dict:
    """
    Получить список товаров из МойСклад.
    """
    url = f"{BASE_URL}/entity/product"
    params = {"limit": limit, "offset": offset}
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()


def get_stock_all(limit: int = 100, offset: int = 0) -> dict:
    """
    Отчет по остаткам: /report/stock/all
    Возвращает словарь, в котором нас интересует ключ 'rows'.
    """
    url = f"{BASE_URL}/report/stock/all"
    params = {"limit": limit, "offset": offset}
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()


def find_product_by_article(article: str) -> dict | None:
    """
    Поиск товара в МойСклад по артикулу.
    Возвращает первую найденную запись или None.
    """
    url = f"{BASE_URL}/entity/product"
    params = {"filter": f"article={article}", "limit": 1}
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    data = r.json()
    rows = data.get("rows", [])
    return rows[0] if rows else None


def create_customer_order(payload: dict):
    url = f"{BASE_URL}/entity/customerorder"
    print("DRY-RUN: отправил бы в МойСклад:")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    # r = requests.post(url, headers=HEADERS, json=payload)
    # r.raise_for_status()
    # return r.json()


if __name__ == "__main__":
    print("=== Тест товаров ===")
    products = get_products(limit=1)
    print(products)

    print("\n=== Тест отчета по остаткам ===")
    stock = get_stock_all(limit=5)
    print(stock)
