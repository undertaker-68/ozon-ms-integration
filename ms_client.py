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
# БАЗОВЫЕ ВСПОМОГАТЕЛИ
# ==========================

def _ms_get(url: str, params: dict | None = None) -> dict:
    """
    Базовый GET к JSON API МойСклад.
    """
    resp = requests.get(url, headers=HEADERS, params=params)
    if not resp.ok:
        raise RuntimeError(
            f"MS GET {url} failed: {resp.status_code} {resp.text}"
        )
    return resp.json()


def _ms_get_by_href(href: str) -> dict:
    """
    Запрос по прямому href объекта в МойСклад.
    Использует общий клиент _ms_get.
    """
    try:
        return _ms_get(href)
    except Exception:
        return {}


# ==========================
# РАСЧЁТ ДОСТУПНОГО ОСТАТКА
# ==========================

def _ms_calc_available(row: dict) -> int:
    """
    Унифицированный расчёт «передаваемого остатка»:
    Переданный остаток = Остаток - Резерв

    Поле "Ожидание" и "Доступно" не используем.
    """
    stock = row.get("stock") or 0
    reserve = row.get("reserve") or 0
    try:
        stock = int(stock)
    except (TypeError, ValueError):
        stock = 0
    try:
        reserve = int(reserve)
    except (TypeError, ValueError):
        reserve = 0
    return max(stock - reserve, 0)


# ==========================
# КОМПЛЕКТЫ
# ==========================

def get_product_bundle_components(bundle_href: str) -> list[dict]:
    """
    Получение состава комплекта по href ассортимента (product / bundle).
    Для обычного товара вернётся пустой список.
    """
    data = _ms_get_by_href(bundle_href)
    if not data:
        return []

    components = data.get("components")
    if isinstance(components, list):
        return components

    # Иногда компоненты лежат во вложенном объекте
    if isinstance(components, dict):
        rows = components.get("rows")
        if isinstance(rows, list):
            return rows

    return []


def calc_bundle_available_for_components(
    components: list[dict],
    stock_by_href: dict[str, int],
) -> int:
    """
    Рассчитываем остаток комплекта по формуле:
      Остаток комплекта = min( остаток_компонента_i / требуемое_кол-во_i )

    stock_by_href — словарь вида {href товара: доступный остаток}
    """
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
        comp_stock = stock_by_href.get(href, 0)
        try:
            qty_required = float(qty_required) if qty_required else 1.0
        except (TypeError, ValueError):
            qty_required = 1.0

        if qty_required <= 0:
            # На всякий пожарный
            qty_required = 1.0

        amount = int(comp_stock // qty_required)
        amounts.append(amount)

    if not amounts:
        return 0

    return max(min(amounts), 0)


def compute_bundle_available(bundle_row: dict, stock_by_href: dict[str, int]) -> int:
    """
    Высокоуровневый помощник:
    на вход – строка ассортимента комплекта + словарь остатков по href,
    на выход – доступный остаток комплекта по компоненту с наименьшим количеством.
    """
    meta = bundle_row.get("meta") or bundle_row.get("assortment", {}).get("meta") or {}
    href = meta.get("href")
    if not href:
        return 0

    components = get_product_bundle_components(href)
    return calc_bundle_available_for_components(components, stock_by_href)


# ==========================
# ОСТАТКИ
# ==========================

def get_stock_by_assortment_href(assortment_href: str) -> dict | None:
    """
    Получить информацию по остаткам конкретной позиции ассортимента
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
    # В отчёте stock/all есть поля stock и reserve
    available = _ms_calc_available(row)
    row["available_for_ozon"] = available
    return row


def get_stock_by_article(article: str) -> dict | None:
    """
    Получить остатки по артикулу через отчёт /report/stock/all
    по складу Ozon.
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
    available = _ms_calc_available(row)
    row["available_for_ozon"] = available
    return row


def get_stock_all(limit: int = 1000, offset: int = 0, store_id: str | None = None) -> dict:
    """
    Обёртка над /entity/assortment, которая подставляет склад
    ЧЕРЕЗ ФИЛЬТР `filter=...;stockStore=<href>`.

    ВАЖНО:
      параметр `stockStore` в корне query (?stockStore=...)
      МойСклад игнорирует для ассортимента и возвращает суммарные
      остатки по всем складам. Поэтому его больше не используем.

    Здесь мы только подставляем фильтр по складу и возвращаем «сырые»
    данные, а sync_stock сам посчитает Остаток - Резерв.
    """
    url = f"{BASE_URL}/entity/assortment"
    params: dict = {
        "limit": limit,
        "offset": offset,
        "expand": "assortment",
    }

    # href нужного склада
    if store_id:
        store_href = f"{BASE_URL}/entity/store/{store_id}"
    else:
        store_href = MS_OZON_STORE_HREF

    # аккуратно дописываем фильтр (если вдруг он уже был)
    base_filter = params.get("filter")
    if base_filter:
        base_filter = f"{base_filter};stockStore={store_href}"
    else:
        base_filter = f"stockStore={store_href}"
    params["filter"] = base_filter

    data = _ms_get(url, params)
    return data


# ==========================
# ЗАКАЗЫ / ОТГРУЗКИ (если есть)
# ==========================

def create_customer_order(payload: dict) -> dict:
    """
    Пример обёртки создания Заказа покупателя (если вдруг нужно).
    """
    url = f"{BASE_URL}/entity/customerorder"
    resp = requests.post(url, headers=HEADERS, json=payload)
    if not resp.ok:
        raise RuntimeError(
            f"MS POST {url} failed: {resp.status_code} {resp.text}"
        )
    return resp.json()
