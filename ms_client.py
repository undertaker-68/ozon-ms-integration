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
# 🔎 Поиск товара / комплекта по артикулу
# -------------------------------------------------
def find_product_by_article(article: str):
    if not article:
        return None

    # 1) Ищем product
    try:
        url = f"{BASE_URL}/entity/product"
        resp = session.get(url, params={"filter": f"article={article}", "limit": 1})
        rows = resp.json().get("rows") or []
        if rows:
            return rows[0]
    except Exception:
        pass

    # 2) Ищем bundle
    try:
        url = f"{BASE_URL}/entity/bundle"
        resp = session.get(url, params={"filter": f"article={article}", "limit": 1})
        rows = resp.json().get("rows") or []
        if rows:
            return rows[0]
    except Exception:
        pass

    return None


# -------------------------------------------------
# 📦 Получение ассортимента по складу
# -------------------------------------------------
def get_assortment(store_id: str):
    url = f"{BASE_URL}/report/stock/all"
    params = {"store.id": store_id, "limit": 1000, "offset": 0}

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
# 📦 Получение состава комплекта
# -------------------------------------------------
def get_bundle_components(bundle_meta_href: str):
    try:
        resp = session.get(f"{bundle_meta_href}/components")
        data = resp.json()

        result = []
        for c in data.get("rows") or []:
            if "assortment" in c and "meta" in c["assortment"]:
                result.append({
                    "meta": c["assortment"]["meta"],
                    "quantity": c.get("quantity", 1)
                })

        return result
    except Exception:
        return []


# =================================================
# 🔥 --- ВОССТАНОВЛЕННЫЕ ФУНКЦИИ ДЛЯ sync_stock.py ---
# =================================================

def get_stock_all(store_id: str):
    """
    Возвращает словарь SKU → остаток.
    Взято из ассортимента.
    """
    rows = get_assortment(store_id)

    stock = {}
    for r in rows:
        article = r.get("article") or r.get("code")
        if not article:
            continue

        stock[article] = {
            "quantity": r.get("stock", 0),
            "reserve": r.get("reserve", 0),
            "free": (r.get("stock", 0) - r.get("reserve", 0)),
            "meta": r.get("meta"),
            "isBundle": r.get("meta", {}).get("type") == "bundle"
        }

    return stock


def compute_bundle_available(bundle_meta_href: str, stock_dict: dict):
    """
    Корректный расчёт комплекта:
    Остаток = min(остаток компонента / требуемое_количество)
    """
    components = get_bundle_components(bundle_meta_href)
    if not components:
        return 0

    available_list = []

    for comp in components:
        meta = comp["meta"]
        qty_needed = comp["quantity"]

        comp_href = meta.get("href")
        if not comp_href:
            available_list.append(0)
            continue

        # находим строку в stock_dict по href
        found_free = None
        for art, row in stock_dict.items():
            if row.get("meta", {}).get("href") == comp_href:
                found_free = row["free"]
                break

        if found_free is None:
            available_list.append(0)
        else:
            available_list.append(found_free // qty_needed)

    return min(available_list) if available_list else 0


# -------------------------------------------------
# 📄 Создание заказа
# -------------------------------------------------
def create_customer_order(payload: dict):
    resp = session.post(f"{BASE_URL}/entity/customerorder", json=payload)
    resp.raise_for_status()
    return resp.json()


# -------------------------------------------------
# 🔍 Поиск заказа
# -------------------------------------------------
def find_customer_order_by_name(name: str):
    resp = session.get(f"{BASE_URL}/entity/customerorder",
                       params={"filter": f"name={name}", "limit": 1})
    rows = resp.json().get("rows") or []
    return rows[0] if rows else None


# -------------------------------------------------
# 🔄 Обновление состояния заказа
# -------------------------------------------------
def update_customer_order_state(order_id: str, state_href: str):
    payload = {
        "state": {
            "meta": {
                "href": state_href,
                "type": "state",
                "mediaType": "application/json"
            }
        }
    }
    resp = session.put(f"{BASE_URL}/entity/customerorder/{order_id}", json=payload)
    resp.raise_for_status()
    return resp.json()


# -------------------------------------------------
# ❗ Снятие резерва
# -------------------------------------------------
def clear_reserve_for_order(order_id: str):
    resp = session.put(
        f"{BASE_URL}/entity/customerorder/{order_id}",
        json={"reservedSum": 0}
    )
    resp.raise_for_status()
    return resp.json()


# -------------------------------------------------
# 🚚 Создание отгрузки createDemand
# -------------------------------------------------
def create_demand_from_order(order_obj: dict):
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
