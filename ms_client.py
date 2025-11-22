import base64
import os
import requests
from dotenv import load_dotenv
import json

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


def create_customer_order(payload: dict) -> dict:
    """
    СОЗДАНИЕ заказа покупателя в МойСклад.
    В боевом режиме сюда будем передавать order_payload из sync_orders.py.
    """
    url = f"{BASE_URL}/entity/customerorder"
    r = requests.post(url, headers=HEADERS, json=payload)
    r.raise_for_status()
    return r.json()


def find_customer_order_by_name(name: str) -> dict | None:
    """
    Поиск заказа покупателя по полю name.
    """
    url = f"{BASE_URL}/entity/customerorder"
    params = {"filter": f"name={name}", "limit": 1}
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    data = r.json()
    rows = data.get("rows", [])
    return rows[0] if rows else None


def update_customer_order_state(order_meta_href: str, state_meta_href: str) -> dict:
    """
    Смена статуса (state) заказа покупателя.
    order_meta_href — meta.href самого заказа (из поля meta заказа).
    state_meta_href — meta.href нужного статуса (надо будет взять из /metadata).
    """
    r = requests.get(order_meta_href, headers=HEADERS)
    r.raise_for_status()
    order = r.json()

    order["state"] = {
        "meta": {
            "href": state_meta_href
        }
    }

    r_put = requests.put(order_meta_href, headers=HEADERS, json=order)
    r_put.raise_for_status()
    return r_put.json()


def clear_reserve_for_order(order_meta_href: str) -> dict:
    """
    Снять резерв по всем позициям заказа.
    """
    r = requests.get(order_meta_href, headers=HEADERS)
    r.raise_for_status()
    order = r.json()

    positions = order.get("positions", [])
    for pos in positions:
        # reserve может отсутствовать — тогда считаем, что 0
        pos["reserve"] = 0

    order["positions"] = positions

    r_put = requests.put(order_meta_href, headers=HEADERS, json=order)
    r_put.raise_for_status()
    return r_put.json()


def create_demand_from_order(order_meta_href: str) -> dict:
    """
    Создать документ 'Отгрузка' (demand) на основе заказа покупателя.
    Берём все позиции из заказа, копируем quantity и assortment.meta.
    """
    # 1. Получаем заказ целиком
    r = requests.get(order_meta_href, headers=HEADERS)
    r.raise_for_status()
    order = r.json()

    demand_payload = {
        "customerOrder": {
            "meta": order["meta"]
        },
        "organization": order.get("organization"),
        "agent": order.get("agent"),
        "store": order.get("store"),
        "positions": [],
    }

    for pos in order.get("positions", []):
        demand_payload["positions"].append({
            "quantity": pos.get("quantity", 0),
            "assortment": pos.get("assortment"),
        })

    url = f"{BASE_URL}/entity/demand"
    r_post = requests.post(url, headers=HEADERS, json=demand_payload)
    r_post.raise_for_status()
    return r_post.json()

if __name__ == "__main__":
    print("=== Тест товаров ===")
    products = get_products(limit=1)
    print(products)

    print("\n=== Тест отчета по остаткам ===")
    stock = get_stock_all(limit=5)
    print(stock)
