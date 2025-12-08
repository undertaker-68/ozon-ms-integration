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

MS_BASE_URL = os.getenv("MS_BASE_URL", "https://api.moysklad.ru/api/remap/1.2")
BASE_URL = MS_BASE_URL.rstrip("/")

MS_OZON_STORE_HREF = os.getenv("MS_OZON_STORE_HREF")  # опционально, для отчётов stock/all

auth_str = f"{MS_LOGIN}:{MS_PASSWORD}"
auth_bytes = auth_str.encode("utf-8")
auth_b64 = base64.b64encode(auth_bytes).decode("utf-8")

HEADERS = {
    "Authorization": f"Basic {auth_b64}",
    "Content-Type": "application/json",
    "Accept-Encoding": "gzip",
}


# ==========================
# БАЗОВЫЕ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
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


# ==========================
# ПОИСК ТОВАРОВ / КЛИЕНТОВ (фрагмент, который у тебя уже был)
# ==========================

def find_product_by_article(article: str) -> dict | None:
    """
    Найти товар по артикулу через /entity/assortment.
    """
    url = f"{BASE_URL}/entity/assortment"
    params = {
        "filter": f"article={article}",
        "limit": 1,
    }
    data = _ms_get(url, params)
    rows = data.get("rows") or []
    if not rows:
        return None
    return rows[0]


def find_counterparty_by_name_or_phone(query: str) -> dict | None:
    """
    Найти контрагента по имени или телефону.
    """
    url = f"{BASE_URL}/entity/counterparty"
    params = {
        "search": query,
        "limit": 1,
    }
    data = _ms_get(url, params)
    rows = data.get("rows") or []
    if not rows:
        return None
    return rows[0]


# ==========================
# КОМПЛЕКТЫ
# ==========================

def compute_bundle_available(bundle_row: dict, stock_by_href: dict[str, int]) -> int:
    """
    Рассчитывает количество доступных комплектов.
    Формула: min(available(component_i) // required_qty_i)

    bundle_row – строка ассортимента комплекта, включающая components.
    Структура components бывает разной: элементы могут быть dict или str (href).

    stock_by_href – словарь {assortment.meta.href -> доступный остаток по КОНКРЕТНОМУ складу}
    """
    components = bundle_row.get("components") or []
    if not components:
        return 0

    amounts: list[int] = []

    for comp in components:
        qty_required = 1
        href: str | None = None

        # Вариант 1: компонент – словарь
        if isinstance(comp, dict):
            qty_required = comp.get("quantity", 1) or 1

            assort = comp.get("assortment")
            if isinstance(assort, dict):
                meta = assort.get("meta", assort) or {}
                if isinstance(meta, dict):
                    href = meta.get("href")
            elif isinstance(assort, str):
                # Иногда assortment может быть сразу href строкой
                href = assort

        # Вариант 2: компонент – просто href строкой
        elif isinstance(comp, str):
            href = comp
            qty_required = 1

        # Если так и не получили href – пропускаем компонент
        if not href:
            continue

        # Берём остаток по этому href из заранее собранного словаря
        available = stock_by_href.get(href, 0)

        try:
            available = int(available)
        except Exception:
            available = 0

        if qty_required <= 0:
            qty_required = 1

        amounts.append(available // qty_required)

    if not amounts:
        return 0

    return min(amounts)


# ==========================
# ОСТАТКИ
# ==========================

def get_stock_by_assortment_href(assortment_href: str) -> int | None:
    """
    Получить текущий остаток товара по meta.href ассортимента
    через отчёт /report/stock/all ТОЛЬКО по складу Ozon.

    Формула:
      Переданный остаток = Остаток - Резерв

    Здесь:
      stock   — поле «Остаток» в отчёте
      reserve — поле «Резерв» в отчёте

    Поля available / inTransit / quantity не используем.
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

    stock_raw = row.get("stock")
    reserve_raw = row.get("reserve", 0)

    try:
        stock_val = int(stock_raw or 0)
    except Exception:
        stock_val = 0

    try:
        reserve_val = int(reserve_raw or 0)
    except Exception:
        reserve_val = 0

    available = stock_val - reserve_val
    if available < 0:
        available = 0

    return available


def get_stock_by_article(article: str) -> int | None:
    """
    Получить остаток по артикулу (через отчёт /report/stock/all, по складу Ozon).

    Формула:
      Переданный остаток = Остаток - Резерв
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

    stock_raw = row.get("stock")
    reserve_raw = row.get("reserve", 0)

    try:
        stock_val = int(stock_raw or 0)
    except Exception:
        stock_val = 0

    try:
        reserve_val = int(reserve_raw or 0)
    except Exception:
        reserve_val = 0

    available = stock_val - reserve_val
    if available < 0:
        available = 0

    return available


def get_stock_all(limit: int = 1000, offset: int = 0, store_id: str | None = None) -> dict:
    """
    Обёртка над /entity/assortment, которая подставляет склад (stockStore)
    и отдаёт «сырые» данные, чтобы sync_stock сам их разобрал.

    ВАЖНО:
      Здесь мы не используем поля quantity / available внутри.
      sync_stock сам считает остаток по формуле «Остаток - Резерв»
      для каждой строки по выбранному складу.
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
# ЗАКАЗЫ / ОТГРУЗКИ
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
    url = order_href
    return _ms_put(url, payload)


def find_customer_order_by_name(name: str) -> dict | None:
    """
    Найти заказ покупателя по номеру (name).
    """
    url = f"{BASE_URL}/entity/customerorder"
    params = {
        "filter": f"name={name}",
        "limit": 1,
    }
    data = _ms_get(url, params)
    rows = data.get("rows") or []
    if not rows:
        return None
    return rows[0]


def update_customer_order_state(order_href: str, state_href: str) -> None:
    """
    Обновить состояние заказа покупателя.
    """
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
    """
    Снять резерв по всем позициям заказа.
    """
    payload = {
        "positions": [],
        "reservedSum": 0,
    }
    _ms_put(order_href, payload)


def create_demand_from_order(order: dict) -> dict:
    """
    Создать отгрузку (demand) на основании заказа покупателя.
    """
    url = f"{BASE_URL}/entity/demand/new"
    params = {
        "customerOrder": order.get("meta", {}).get("href"),
    }
    data = _ms_get(url, params)

    # Можем при необходимости поправить payload перед созданием.
    demand_payload = data
    url = f"{BASE_URL}/entity/demand"
    r_post = requests.post(url, headers=HEADERS, json=demand_payload, timeout=30)
    if r_post.status_code >= 400:
        print(f"[MS ERROR] create_demand status={r_post.status_code} body={r_post.text[:500]}")
    r_post.raise_for_status()
    return r_post.json()
