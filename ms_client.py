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


def _ms_get_by_href(href: str) -> dict:
    """
    Запрос по прямому href объекта в МойСклад.
    Использует общий клиент _ms_get.
    """
    try:
        return _ms_get(href)
    except Exception:
        return {}


def compute_bundle_available(bundle_row: dict) -> int:
    """
    Рассчитывает количество доступных комплектов.
    Формула: min(available(component_i) // required_qty_i)

    bundle_row – строка ассортимента комплекта, включающая components.
    """
    components = bundle_row.get("components") or []
    if not components:
        return 0

    amounts = []

    for comp in components:
        qty_required = comp.get("quantity", 1)

        assort = comp.get("assortment", {}).get("meta", {})
        href = assort.get("href")
        if not href:
            continue

        comp_data = _ms_get_by_href(href)

        # Берём 'available' — если нет, fallback на 'stock'
        available = comp_data.get("available")
        if available is None:
            available = comp_data.get("stock", 0)

        try:
            available = int(available)
        except Exception:
            available = 0

        # Сколько комплектов может дать этот компонент
        amounts.append(available // qty_required)

    if not amounts:
        return 0

    return max(min(amounts), 0)


# ==========================
# УТИЛИТЫ
# ==========================

def _ms_get(url: str, params: dict | None = None) -> dict:
    """
    Универсальный GET к МойСклад.
    Логируем только ошибки.
    """
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    if r.status_code >= 400:
        print(f"[MS GET ERROR] {r.url} status={r.status_code} body={r.text[:500]}")
    r.raise_for_status()
    return r.json()


def _ms_post(url: str, json_data: dict) -> dict:
    """
    Универсальный POST к МойСклад.
    """
    r = requests.post(url, headers=HEADERS, json=json_data, timeout=30)
    if r.status_code >= 400:
        print(f"[MS POST ERROR] {r.url} status={r.status_code} body={r.text[:500]}")
    r.raise_for_status()
    return r.json()


def _ms_put(url: str, json_data: dict) -> dict:
    """
    Универсальный PUT к МойСклад.
    """
    r = requests.put(url, headers=HEADERS, json=json_data, timeout=30)
    if r.status_code >= 400:
        print(f"[MS PUT ERROR] {r.url} status={r.status_code} body={r.text[:500]}")
    r.raise_for_status()
    return r.json()


def _ms_get_list(url: str, params: dict | None = None) -> list[dict]:
    """
    Получить постранично список сущностей.
    """
    limit = params.get("limit", 1000) if params else 1000
    offset = params.get("offset", 0) if params else 0

    items: list[dict] = []

    while True:
        p = dict(params or {})
        p["limit"] = limit
        p["offset"] = offset

        data = _ms_get(url, p)
        rows = data.get("rows") or data.get("reports") or []
        if not rows:
            break

        items.extend(rows)

        if len(rows) < limit:
            break

        offset += limit

    return items


# ==========================
# ОСТАТКИ
# ==========================

def get_stock_by_assortment_href(assortment_href: str) -> int | None:
    """
    Получить текущий остаток товара по meta.href ассортимента
    через отчет /report/stock/all ТОЛЬКО по складу Ozon.
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
    # В отчёте stock/all есть поля stock, quantity, reserve, available и т.п.
    # Нас интересует доступный остаток.
    available = row.get("available")
    if available is None:
        available = row.get("stock", 0)

    try:
        return int(available)
    except Exception:
        return 0


def get_stock_by_article(article: str) -> int | None:
    """
    Получить остаток по артикулу (через отчёт /report/stock/all, по складу Ozon).
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
    available = row.get("available")
    if available is None:
        available = row.get("stock", 0)

    try:
        return int(available)
    except Exception:
        return 0


def get_stock_all(limit: int = 1000, offset: int = 0, store_id: str | None = None) -> dict:
    """
    Обёртка над /entity/assortment, которая подставляет склад (stockStore)
    и отдаёт «сырые» данные, чтобы sync_stock сам их разобрал.
    """
    url = f"{BASE_URL}/entity/assortment"
    params: dict = {
        "limit": limit,
        "offset": offset,
        "expand": "assortment",
    }

    if store_id:
        params["stockStore"] = f"{BASE_URL}/entity/store/{store_id}"
    else:
        params["stockStore"] = MS_OZON_STORE_HREF

    data = _ms_get(url, params)
    return data


# ==========================
# ЗАКАЗЫ / ОТГРУЗКИ (если есть)
# ==========================

def create_customer_order(payload: dict) -> dict:
    """
    Создать заказ покупателя в МойСклад.
    """
    url = f"{BASE_URL}/entity/customerorder"
    return _ms_post(url, payload)


def update_customer_order(order_href: str, payload: dict) -> dict:
    """
    Обновить заказ покупателя (по meta.href).
    """
    return _ms_put(order_href, payload)


def create_demand_from_customer_order(order: dict) -> dict:
    """
    Простая реализация создания отгрузки на основании заказа покупателя.
    """
    demand_payload = {
        "customerOrder": order.get("meta"),
        "organization": order.get("organization"),
        "agent": order.get("agent"),
        "store": order.get("store"),
        "positions": [],
    }

    for pos in order.get("positions", []):
        demand_payload["positions"].append(
            {
                "quantity": pos.get("quantity", 0),
                "assortment": pos.get("assortment"),
            }
        )

    url = f"{BASE_URL}/entity/demand"
    r_post = requests.post(url, headers=HEADERS, json=demand_payload, timeout=30)
    if r_post.status_code >= 400:
        print(f"[MS ERROR] create_demand status={r_post.status_code} body={r_post.text[:500]}")
    r_post.raise_for_status()

    return r_post.json()
