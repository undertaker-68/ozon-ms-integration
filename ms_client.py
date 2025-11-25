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


import requests
import json

# предполагаю, что у тебя уже есть MS_AUTH / HEADERS для МойСклад.
# Если HEADERS уже определён выше в файле — вторую дефиницию не трогай.
MS_BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
# HEADERS = {...}  # уже должен быть у тебя в ms_client.py


def _ms_get(url: str, params: dict | None = None) -> dict:
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    print(f"Запрос к МС: {r.url}")
    print("Статус:", r.status_code)
    if r.status_code >= 400:
        print("Ответ МС:", r.text[:2000])
    r.raise_for_status()
    return r.json()


def find_product_by_article(article: str) -> dict | None:
    """
    Пытается найти товар в МойСклад по артикулу из Ozon.

    Логика:
    1) Сначала делаем /entity/product?search=<article>  (частичный поиск по всем полям).
       Если находит — берём первый товар.
    2) Если ничего не нашлось — пробуем точные фильтры:
       article=..., code=..., name=...
    """

    base_url = f"{MS_BASE_URL}/entity/product"

    # 0. Универсальный search
    try:
        params = {"search": article}
        data = _ms_get(base_url, params=params)
        rows = data.get("rows", [])
        if rows:
            print(f"Найден товар в МС по search={article}: {rows[0].get('name')} (article={rows[0].get('article')}, code={rows[0].get('code')})")
            return rows[0]
        else:
            print(f"МС: search={article} вернул 0 товаров")
    except Exception as e:
        print(f"Ошибка поиска в МС по search={article}: {e!r}")

    # 1. Точный article
    try:
        params = {"filter": f"article={article}"}
        data = _ms_get(base_url, params=params)
        rows = data.get("rows", [])
        if rows:
            print(f"Найден товар в МС по article={article}")
            return rows[0]
    except Exception as e:
        print(f"Ошибка поиска по article={article}: {e!r}")

    # 2. Точный code
    try:
        params = {"filter": f"code={article}"}
        data = _ms_get(base_url, params=params)
        rows = data.get("rows", [])
        if rows:
            print(f"Найден товар в МС по code={article}")
            return rows[0]
    except Exception as e:
        print(f"Ошибка поиска по code={article}: {e!r}")

    # 3. Точный name
    try:
        params = {"filter": f"name={article}"}
        data = _ms_get(base_url, params=params)
        rows = data.get("rows", [])
        if rows:
            print(f"Найден товар в МС по name={article}")
            return rows[0]
    except Exception as e:
        print(f"Ошибка поиска по name={article}: {e!r}")

    print(f"Товар в МойСклад не найден ни по search/article/code/name = {article}")
    return None


def create_customer_order(payload: dict) -> dict:
    url = "https://api.moysklad.ru/api/remap/1.2/entity/customerorder"
    print("=== Запрос в МойСклад /entity/customerorder ===")
    print("URL:", url)
    print("Тело запроса (фрагмент):")
    try:
        print(json.dumps(payload, ensure_ascii=False, indent=2)[:2000])
    except Exception:
        print(str(payload)[:2000])
    print("=== /Запрос ===")

    r = requests.post(url, json=payload, headers=HEADERS, timeout=30)

    print("=== Ответ МойСклад /entity/customerorder ===")
    print("HTTP status:", r.status_code)
    print("Тело ответа:")
    print(r.text)
    print("=== /Ответ ===")

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
