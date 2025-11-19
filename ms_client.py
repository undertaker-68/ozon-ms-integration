import base64
import requests
from dotenv import load_dotenv
import os

load_dotenv()

MS_LOGIN = os.getenv("MS_LOGIN")
MS_PASSWORD = os.getenv("MS_PASSWORD")

if not MS_LOGIN or not MS_PASSWORD:
    raise RuntimeError("Не заданы MS_LOGIN / MS_PASSWORD в .env")

AUTH = base64.b64encode(f"{MS_LOGIN}:{MS_PASSWORD}".encode("utf-8")).decode("utf-8")

HEADERS = {
    "Authorization": f"Basic {AUTH}",
    "Accept": "application/json;charset=utf-8",
    "Accept-Encoding": "gzip",
}

BASE_URL = "https://api.moysklad.ru/api/remap/1.2"


def get_products(limit=10, offset=0):
    """
    Простой тестовый метод: получаем товары.
    """
    url = f"{BASE_URL}/entity/product"
    params = {"limit": limit, "offset": offset}
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()


def get_stock_all(limit=100, offset=0):
    """
    Отчет по остаткам: /report/stock/all
    Здесь МойСклад отдаёт по позициям: article, stock, quantity и т.п.
    """
    url = f"{BASE_URL}/report/stock/all"
    params = {"limit": limit, "offset": offset}
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()


if __name__ == "__main__":
    # Тест: покажем 1 товар и 5 строк отчёта по остаткам
    print("=== Тест товаров ===")
    products = get_products(limit=1)
    print(products)

    print("\n=== Тест остатков ===")
    stock = get_stock_all(limit=5)
    print(stock)

