import os
import requests
from dotenv import load_dotenv

load_dotenv()

MS_LOGIN = os.getenv("MS_LOGIN")
MS_PASSWORD = os.getenv("MS_PASSWORD")

BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
AUTH = (MS_LOGIN, MS_PASSWORD)

session = requests.Session()
session.auth = AUTH
session.headers.update({
    "Content-Type": "application/json",
    "Accept-Encoding": "gzip"
})


# -------------------------------------------------
# 🔎  ✔ Функция поиска товара по артикулу (добавлена)
# -------------------------------------------------
def find_product_by_article(article: str):
    """
    Находит товар или комплект (bundle) в МойСклад по артикулу.
    Возвращает объект с meta.
    """
    if not article:
        return None

    # ---- Сначала ищем обычный товар ----
    url = f"{BASE_URL}/entity/product"
    params = {"filter": f"article={article}", "limit": 1}

    try:
        resp = session.get(url, params=params)
        data = resp.json()
        rows = data.get("rows") or []
        if rows:
            return rows[0]
    except Exception as e:
        print(f"[MS] Ошибка поиска товара: {e}")

    # ---- Если нет — ищем комплект ----
    url = f"{BASE_URL}/entity/bundle"
    try:
        resp = session.get(url, params=params)
        data = resp.json()
        rows = data.get("rows") or []
        if rows:
            return rows[0]
    except Exception:
        pass

    return None


# -------------------------------------------------
# 🌟 Получение ассортимента + остатков по складу
# -------------------------------------------------
def get_assortment(store_id: str):
    """
    Возвращает список позиций на складе:
    - products
    - bundles
    - services (при необходимости)
    """
    url = f"{BASE_URL}/report/stock/all"
    params = {
        "store.id": store_id,
        "limit": 1000,
        "offset": 0
    }

    all_rows = []

    while True:
        resp = session.get(url, params=params)
        data = resp.json()

        rows = data.get("rows") or []
        all_rows.extend(rows)

        if len(rows) < 1000:
            break

        params["offset"] += 1000

    return all_rows


# -------------------------------------------------
# 📦 Получение состава комплекта (bundle)
# -------------------------------------------------
def get_bundle_components(bundle_meta_href: str):
    """
    Получение компонентов комплекта (bundle):
    Возвращает:
      [
         { "meta": {...}, "quantity": X },
         ...
      ]
    """
    try:
        url = f"{bundle_meta_href}/components"
        resp = session.get(url)
        data = resp.json()

        return [
            {
                "meta": c.get("assortment", {}).get("meta"),
                "quantity": c.get("quantity", 1)
            }
            for c in (data.get("rows") or [])
            if c.get("assortment", {}).get("meta")
        ]
    except Exception:
        return []


# -------------------------------------------------
# 📦 Создание заказа
# -------------------------------------------------
def create_customer_order(payload: dict):
    url = f"{BASE_URL}/entity/customerorder"
    resp = session.post(url, json=payload)
    resp.raise_for_status()
    return resp.json()


# -------------------------------------------------
# 🔍 Поиск заказа по номеру
# -------------------------------------------------
def find_customer_order_by_name(name: str):
    url = f"{BASE_URL}/entity/customerorder"
    params = {"filter": f"name={name}", "limit": 1}

    resp = session.get(url, params=params)
    data = resp.json()
    rows = data.get("rows") or []
    return rows[0] if rows else None


# -------------------------------------------------
# 🔄 Обновление состояния заказа
# -------------------------------------------------
def update_customer_order_state(order_id: str, state_href: str):
    url = f"{BASE_URL}/entity/customerorder/{order_id}"
    payload = {
        "state": {
            "meta": {
                "href": state_href,
                "type": "state",
                "mediaType": "application/json"
            }
        }
    }

    resp = session.put(url, json=payload)
    resp.raise_for_status()
    return resp.json()


# -------------------------------------------------
# 🔥 Снятие резерва
# -------------------------------------------------
def clear_reserve_for_order(order_id: str):
    """
    Удаляет резерв из заказа (если нужно).
    """
    url = f"{BASE_URL}/entity/customerorder/{order_id}"
    payload = {"reservedSum": 0}

    resp = session.put(url, json=payload)
    resp.raise_for_status()
    return resp.json()


# -------------------------------------------------
# 🚚 Создание отгрузки (demand) из заказа
# -------------------------------------------------
def create_demand_from_order(order_obj: dict):
    """
    Создаёт отгрузку (demand) из объекта заказа.
    order_obj — это JSON заказа (не ссылка).
    """
    meta = order_obj.get("meta")
    if not meta:
        raise ValueError("order_obj.meta отсутствует")

    order_href = meta.get("href")
    if not order_href:
        raise ValueError("order.meta.href отсутствует")

    url = f"{order_href}/createDemand"

    resp = session.post(url)
    resp.raise_for_status()

    return resp.json()
