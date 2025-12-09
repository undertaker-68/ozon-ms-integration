import os
import requests
from dotenv import load_dotenv

load_dotenv()

MS_LOGIN = os.getenv("MS_LOGIN")
MS_PASSWORD = os.getenv("MS_PASSWORD")

BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
AUTH = (MS_LOGIN, MS_PASSWORD)

# Для совместимости с логикой склада Ozon
MS_OZON_STORE_ID = os.getenv("MS_OZON_STORE_ID")
MS_OZON_STORE_HREF = os.getenv("MS_OZON_STORE_HREF")

session = requests.Session()
session.auth = AUTH
session.headers.update(
    {
        "Accept-Encoding": "gzip",
        "Content-Type": "application/json",
    }
)


# -------------------------------------------------
# 🔹 Универсальный пагинатор для /entity/*
# -------------------------------------------------
def _get_all_entities(url: str, params: dict | None = None):
    if params is None:
        params = {}

    limit = params.get("limit", 1000)
    offset = params.get("offset", 0)
    all_rows = []

    while True:
        local_params = params.copy()
        local_params["limit"] = limit
        local_params["offset"] = offset

        resp = session.get(url, params=local_params)
        resp.raise_for_status()
        data = resp.json()

        rows = data.get("rows", [])
        all_rows.extend(rows)

        if len(rows) < limit:
            break

        offset += limit

    return all_rows


# -------------------------------------------------
# 🔸 Ассортимент /report/stock/all (СЕЙЧАС НЕ ИСПОЛЬЗУЕТСЯ sync_stock)
# -------------------------------------------------
def get_assortment(url_params: dict):
    """
    Оставляем как есть для совместимости с другим кодом.
    sync_stock.py использует get_stock_all(), см. ниже.
    """
    url = f"{BASE_URL}/report/stock/all"
    limit = 1000
    offset = 0
    rows = []

    while True:
        params = {"limit": limit, "offset": offset, **url_params}
        response = session.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        rows.extend(data.get("rows", []))

        if len(data.get("rows", [])) < limit:
            break

        offset += limit

    return rows


# -------------------------------------------------
# 🔸 Остатки по одному складу через /entity/assortment
#     (ИСПОЛЬЗУЕТСЯ sync_stock.py)
# -------------------------------------------------
def get_stock_all(limit=1000, offset=0, store_id=None):
    """
    Обёртка над /entity/assortment с параметром stockStore — остатки по ОДНОМУ складу.
    Именно эту функцию вызывает sync_stock.py.

    Возвращает dict с полями:
      - rows: список строк ассортимента
      - meta: метаинформация (href, size и т.п.)
    """
    params = {
        "limit": limit,
        "offset": offset,
        # Чтобы приходили поля stock, reserve, inTransit, quantity
        "stockMode": "all",
    }

    # Фильтр по складу:
    #  - если передан store_id → берём конкретный склад,
    #  - иначе fallback на MS_OZON_STORE_HREF (как было в старом коде).
    stock_store_href = None
    if store_id:
        stock_store_href = f"{BASE_URL}/entity/store/{store_id}"
    elif MS_OZON_STORE_HREF:
        stock_store_href = MS_OZON_STORE_HREF

    if stock_store_href:
        params["stockStore"] = stock_store_href

    url = f"{BASE_URL}/entity/assortment"
    resp = session.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


# -------------------------------------------------
# 🔹 Поиск товара по артикулу
# -------------------------------------------------
def find_product_by_article(article: str):
    """
    Поиск товара по артикулу (код товара в МойСклад = article).
    Возвращает первую найденную позицию или None.
    """
    url = f"{BASE_URL}/entity/product"
    params = {
        "filter": f"article={article}",
        "limit": 1,
    }
    resp = session.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    rows = data.get("rows", [])
    return rows[0] if rows else None


# -------------------------------------------------
# 🔹 Работа с заказами покупателей
# -------------------------------------------------
def get_customer_order_by_name(name: str):
    """
    Поиск заказа покупателя по номеру (name).
    Возвращает dict заказа или None.
    """
    url = f"{BASE_URL}/entity/customerorder"
    params = {
        "filter": f"name={name}",
        "limit": 1,
    }
    resp = session.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    rows = data.get("rows", [])
    return rows[0] if rows else None


def create_customer_order(payload: dict) -> dict:
    """
    Создание заказа покупателя.
    payload — уже подготовленное тело заказа под API МойСклад.
    """
    url = f"{BASE_URL}/entity/customerorder"
    resp = session.post(url, json=payload)
    resp.raise_for_status()
    return resp.json()


def update_customer_order(order_id: str, payload: dict) -> dict:
    """
    Обновление заказа покупателя по id.
    """
    url = f"{BASE_URL}/entity/customerorder/{order_id}"
    resp = session.put(url, json=payload)
    resp.raise_for_status()
    return resp.json()


def set_customer_order_saleschannel(order_href: str, saleschannel_href: str):
    """
    Установка канала продаж (salesChannel) для заказа покупателя.

    :param order_href: meta.href заказа (полный URL на сущность customerorder)
    :param saleschannel_href: meta.href канала продаж
    """
    url = order_href
    body = {
        "salesChannel": {
            "meta": {
                "href": saleschannel_href,
                "type": "saleschannel",
                "mediaType": "application/json",
            }
        }
    }
    resp = session.put(url, json=body)
    resp.raise_for_status()
    return resp.json()


# -------------------------------------------------
# 🔹 Каналы продаж (saleschannel)
# -------------------------------------------------
def get_saleschannel_by_id(saleschannel_id: str):
    """
    Получить канал продаж по UUID.
    """
    url = f"{BASE_URL}/entity/saleschannel/{saleschannel_id}"
    resp = session.get(url)
    resp.raise_for_status()
    return resp.json()


# -------------------------------------------------
# 🔹 Отгрузки (demand)
# -------------------------------------------------
def create_demand_from_order(customer_order_meta: dict, shipment_payload_extra: dict | None = None):
    """
    Создание отгрузки (demand) на основе заказа покупателя.

    :param customer_order_meta: meta-заказа (customerorder["meta"])
    :param shipment_payload_extra: дополнительные поля для отгрузки (warehouse, agent и т.д.)
    """
    url = f"{BASE_URL}/entity/demand"

    payload = {
        "customerOrder": {"meta": customer_order_meta},
    }

    if shipment_payload_extra:
        payload.update(shipment_payload_extra)

    resp = session.post(url, json=payload)
    resp.raise_for_status()
    return resp.json()


def create_demand_direct(payload: dict) -> dict:
    """
    Прямое создание отгрузки (demand) по полному payload.
    """
    url = f"{BASE_URL}/entity/demand"
    resp = session.post(url, json=payload)
    resp.raise_for_status()
    return resp.json()


# -------------------------------------------------
# 🔹 Комплекты (bundle)
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
    url = f"{bundle_meta_href}/components"
    resp = session.get(url)
    resp.raise_for_status()
    data = resp.json()
    return data.get("rows", [])


def compute_bundle_available(components: list[dict], stock_by_product_href: dict[str, int]) -> int:
    """
    Расчёт доступного количества комплекта по компонентам.

    :param components: список компонентов (как вернул get_bundle_components)
    :param stock_by_product_href: словарь {href товара: доступный остаток}
    :return: доступное количество комплекта
    """
    if not components:
        return 0

    bundle_counts = []
    for comp in components:
        product_meta = comp.get("meta", {})
        product_href = product_meta.get("href")
        quantity_in_bundle = comp.get("quantity") or 0

        if not product_href or quantity_in_bundle <= 0:
            continue

        available = stock_by_product_href.get(product_href, 0)
        bundle_counts.append(available // quantity_in_bundle if quantity_in_bundle else 0)

    return min(bundle_counts) if bundle_counts else 0
