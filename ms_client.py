# ms_client.py
import base64
import os
import requests
from dotenv import load_dotenv

load_dotenv()

# ==========================
# БАЗОВЫЕ НАСТРОЙКИ MS
# ==========================

MS_LOGIN = os.getenv("MS_LOGIN")
MS_PASSWORD = os.getenv("MS_PASSWORD")

if not MS_LOGIN or not MS_PASSWORD:
    raise RuntimeError("Не заданы MS_LOGIN / MS_PASSWORD в .env")

BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
MS_AUTH = base64.b64encode(f"{MS_LOGIN}:{MS_PASSWORD}".encode("utf-8")).decode("utf-8")

HEADERS = {
    "Authorization": f"Basic {MS_AUTH}",
    "Accept": "application/json;charset=utf-8",
    "Accept-Encoding": "gzip",
    "Content-Type": "application/json",
}

MS_OZON_STORE_ID = os.getenv("MS_OZON_STORE_ID")
if not MS_OZON_STORE_ID:
    raise RuntimeError("Не задан MS_OZON_STORE_ID в .env")

MS_OZON_STORE_HREF = f"{BASE_URL}/entity/store/{MS_OZON_STORE_ID}"
MS_BASE_URL = BASE_URL


# ==========================
# БАЗОВЫЕ HTTP-ХЕЛПЕРЫ
# ==========================

def _ms_get(url: str, params: dict | None = None) -> dict:
    """
    Универсальный GET к МойСклад.
    """
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    if r.status_code >= 400:
        print(f"[MS GET ERROR] {r.url} status={r.status_code} body={r.text[:500]}")
    r.raise_for_status()
    return r.json()


def _ms_post(url: str, json_data: dict) -> dict:
    r = requests.post(url, headers=HEADERS, json=json_data, timeout=30)
    if r.status_code >= 400:
        print(f"[MS POST ERROR] {r.url} status={r.status_code} body={r.text[:500]}")
    r.raise_for_status()
    return r.json()


def _ms_put(url: str, json_data: dict) -> dict:
    r = requests.put(url, headers=HEADERS, json=json_data, timeout=30)
    if r.status_code >= 400:
        print(f"[MS PUT ERROR] {r.url} status={r.status_code} body={r.text[:500]}")
    r.raise_for_status()
    return r.json()


def _ms_get_by_href(href: str) -> dict:
    """
    Безопасный GET по meta.href.
    """
    try:
        return _ms_get(href)
    except Exception as e:
        print(f"[MS GET BY HREF ERROR] {href}: {e!r}")
        return {}


# ==========================
# КОМПЛЕКТЫ (BUNDLE)
# ==========================

def _get_bundle_components(bundle_row: dict) -> list[dict]:
    """
    Получаем реальные компоненты комплекта.

    В ответе /entity/assortment по комплекту (bundle) мы видим только:
      "components": { "meta": { "href": ".../bundle/<id>/components", ... } }

    Поэтому нужно отдельно сходить по этому href и взять rows.
    """
    comps = bundle_row.get("components")
    if isinstance(comps, dict):
        meta = comps.get("meta") or {}
        href = meta.get("href")
        if href:
            data = _ms_get_by_href(href)
            rows = data.get("rows") or []
            if isinstance(rows, list):
                return rows

    # На всякий случай пробуем через assortment.components (если когда-то будет expand)
    assort = bundle_row.get("assortment")
    if isinstance(assort, dict):
        comps = assort.get("components")
        if isinstance(comps, dict):
            rows = comps.get("rows") or []
            if isinstance(rows, list):
                return rows
        elif isinstance(comps, list):
            return comps

    return []


def compute_bundle_available(bundle_row: dict, stock_by_href: dict[str, int]) -> int:
    """
    Рассчитывает количество доступных комплектов по формуле:
      Остаток комплекта = min( остаток_компонента_i / требуемое_кол-во_i )

    В stock_by_href уже лежит доступный остаток для каждой позиции ассортимента
    по формуле (stock - reserve) для НУЖНОГО склада.
    """
    components = _get_bundle_components(bundle_row)
    if not components:
        # Нет состава – считаем, что комплекта нет
        return 0

    amounts: list[int] = []

    for comp in components:
        qty_required = comp.get("quantity", 1) or 1
        href: str | None = None

        assort = comp.get("assortment")
        if isinstance(assort, dict):
            meta = assort.get("meta", assort) or {}
            if isinstance(meta, dict):
                href = meta.get("href")
        elif isinstance(assort, str):
            href = assort

        if not href:
            continue

        available = stock_by_href.get(href, 0)

        try:
            available = int(available)
        except Exception:
            available = 0

        try:
            qty_required = int(qty_required)
        except Exception:
            qty_required = 1

        if qty_required <= 0:
            qty_required = 1

        amounts.append(max(0, available) // qty_required)

    if not amounts:
        return 0

    result = min(amounts)
    return max(result, 0)


# ==========================
# ОСТАТКИ
# ==========================

def get_stock_all(
    limit: int = 1000,
    offset: int = 0,
    store_id: str | None = None,
) -> dict:
    """
    Читаем /entity/assortment ТОЛЬКО по одному складу.

    Критично: склад задаём НЕ через stockStore=... в корне,
    а через фильтр:

      filter=stockStore=<href склада>

    Иначе МойСклад отдаёт общий остаток по всем складам (что мы и видели:
    00519 stock=127, reserve=1, quantity=126).
    """
    url = f"{BASE_URL}/entity/assortment"

    if store_id:
        store_href = f"{BASE_URL}/entity/store/{store_id}"
    else:
        store_href = MS_OZON_STORE_HREF

    params: dict = {
        "limit": limit,
        "offset": offset,
        "expand": "assortment",
        "filter": f"stockStore={store_href}",
    }

    return _ms_get(url, params)


def get_stock_by_assortment_href(assortment_href: str) -> int | None:
    """
    Точечный остаток по товару через отчёт /report/stock/all по складу Ozon.
    (используется только в единичных местах, массовый синк идёт через get_stock_all)
    """
    url = f"{BASE_URL}/report/stock/all"
    params = {
        "filter": f"assortment={assortment_href}",
        "limit": 1,
        "stockStore": MS_OZON_STORE_HREF,
    }
    data = _ms_get(url, params)
    rows = data.get("rows") or []
    if not rows:
        return None

    row = rows[0]
    stock_val = row.get("stock", 0)
    reserve_val = row.get("reserve", 0)

    try:
        stock_int = int(stock_val or 0)
    except Exception:
        stock_int = 0

    try:
        reserve_int = int(reserve_val or 0)
    except Exception:
        reserve_int = 0

    available = stock_int - reserve_int
    return max(available, 0)


def get_stock_by_article(article: str) -> int | None:
    """
    Точечный остаток по артикулу через отчёт /report/stock/all по складу Ozon.
    """
    url = f"{BASE_URL}/report/stock/all"
    params = {
        "filter": f"article={article}",
        "limit": 1,
        "stockStore": MS_OZON_STORE_HREF,
    }
    data = _ms_get(url, params)
    rows = data.get("rows") or []
    if not rows:
        return None

    row = rows[0]
    stock_val = row.get("stock", 0)
    reserve_val = row.get("reserve", 0)

    try:
        stock_int = int(stock_val or 0)
    except Exception:
        stock_int = 0

    try:
        reserve_int = int(reserve_val or 0)
    except Exception:
        reserve_int = 0

    available = stock_int - reserve_int
    return max(available, 0)


# ==========================
# ПОИСК ТОВАРОВ / КОНТРАГЕНТОВ (для sync_orders)
# ==========================

def find_product_by_article(article: str) -> dict | None:
    url = f"{BASE_URL}/entity/assortment"
    params = {
        "filter": f"article={article}",
        "limit": 1,
    }
    data = _ms_get(url, params)
    rows = data.get("rows") or []
    return rows[0] if rows else None


def find_counterparty_by_name_or_phone(query: str) -> dict | None:
    url = f"{BASE_URL}/entity/counterparty"
    params = {
        "search": query,
        "limit": 1,
    }
    data = _ms_get(url, params)
    rows = data.get("rows") or []
    return rows[0] if rows else None


# ==========================
# ЗАКАЗЫ / ОТГРУЗКИ (для sync_orders)
# ==========================

def create_customer_order(payload: dict) -> dict:
    url = f"{BASE_URL}/entity/customerorder"
    return _ms_post(url, payload)


def update_customer_order(order_href: str, payload: dict) -> dict:
    return _ms_put(order_href, payload)


def find_customer_order_by_name(name: str) -> dict | None:
    url = f"{BASE_URL}/entity/customerorder"
    params = {
        "filter": f"name={name}",
        "limit": 1,
    }
    data = _ms_get(url, params)
    rows = data.get("rows") or []
    return rows[0] if rows else None


def update_customer_order_state(order_href: str, state_href: str) -> None:
    payload = {
        "state": {
            "meta": {
                "href": state_href,
                "type": "state",
                "mediaType": "application/json",
            }
        }
    }
    _ms_put(order_href, payload)


def clear_reserve_for_order(order_href: str) -> None:
    payload = {
        "positions": [],
        "reservedSum": 0,
    }
    _ms_put(order_href, payload)


def create_demand_from_order(order: dict) -> dict:
    """
    Создать отгрузку (demand) на основании заказа покупателя.
    Без /entity/demand/new – сразу POST /entity/demand.
    """
    order_meta = order.get("meta") or {}
    order_href = order_meta.get("href")
    if not order_href:
        raise ValueError("У заказа нет meta.href, не можем создать отгрузку")

    # Если в объекте заказа нет позиций – добираем полный заказ по href
    if not order.get("positions"):
        order = _ms_get_by_href(order_href)

    positions = order.get("positions") or []

    demand_payload = {
        "customerOrder": {"meta": order["meta"]},
        "organization": order["organization"],
        "agent": order["agent"],
        "store": order["store"],
        "positions": [
            {
                "quantity": pos.get("quantity", 0),
                "assortment": pos.get("assortment"),
            }
            for pos in positions
        ],
    }
    
        # === Название отгрузки = номер отправления (имя заказа) ===
    demand_name = order.get("name")
    if demand_name:
        demand_payload["name"] = demand_name

    url = f"{BASE_URL}/entity/demand"
    return _ms_post(url, demand_payload)
