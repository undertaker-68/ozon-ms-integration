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

AUTH = base64.b64encode(f"{MS_LOGIN}:{MS_PASSWORD}".encode("utf-8")).decode("utf-8")

HEADERS = {
    "Authorization": f"Basic {AUTH}",
    "Accept": "application/json;charset=utf-8",
    "Accept-Encoding": "gzip",
}

BASE_URL = "https://api.moysklad.ru/api/remap/1.2"

MS_OZON_STORE_ID = os.getenv("MS_OZON_STORE_ID")
if not MS_OZON_STORE_ID:
    raise RuntimeError("Не задан MS_OZON_STORE_ID в .env")

MS_OZON_STORE_HREF = f"{BASE_URL}/entity/store/{MS_OZON_STORE_ID}"
MS_BASE_URL = BASE_URL


# ==========================
# ВСПОМОГАТЕЛЬНЫЙ GET
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
    data = _ms_get(url, params=params)
    rows = data.get("rows", [])
    if not rows:
        return None

    stock = rows[0].get("stock")
    try:
        return int(stock)
    except (TypeError, ValueError):
        return None


def get_products(limit: int = 10, offset: int = 0) -> dict:
    url = f"{BASE_URL}/entity/product"
    params = {"limit": limit, "offset": offset}
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def get_stock_all(limit: int = 100, offset: int = 0, store_id: str | None = None) -> dict:
    """
    Остатки по ассортименту через /entity/assortment.

    ВАЖНО:
      - Фильтруем ТОЛЬКО по одному складу через filter=stockStore=<href>.
      - stockMode=all, чтобы видеть и нули, и отрицательные остатки.
    """
    url = f"{BASE_URL}/entity/assortment"

    params: dict[str, object] = {
        "limit": limit,
        "offset": offset,
        "stockMode": "all",
    }

    # Если склад не передан явно — используем MS_OZON_STORE_ID
    if store_id is None:
        store_id = MS_OZON_STORE_ID

    # href склада
    if str(store_id).startswith("http"):
        stock_store_href = store_id
    else:
        stock_store_href = f"{BASE_URL}/entity/store/{store_id}"

    # главный момент: фильтр по складу
    params["filter"] = f"stockStore={stock_store_href}"

    data = _ms_get(url, params=params)
    return data


def get_stock_by_article(article: str) -> int | None:
    """
    Получить текущий остаток товара по артикулу (report/stock/all)
    ТОЛЬКО по складу Ozon (через stockStore).
    """
    url = f"{BASE_URL}/report/stock/all"
    params = {
        "filter": f"article={article}",
        "limit": 1,
        "stockStore": MS_OZON_STORE_HREF,
    }

    data = _ms_get(url, params=params)
    rows = data.get("rows", [])
    if not rows:
        return None

    stock = rows[0].get("stock")
    try:
        return int(stock)
    except (TypeError, ValueError):
        return None


# ==========================
# НОРМАЛИЗАЦИЯ АРТИКУЛА
# ==========================

def _normalize_article(s: str | None) -> str:
    """
    Нормализация артикула:
      - убираем пробелы по краям;
      - заменяем визуально одинаковые русские буквы на латиницу;
      - приводим к верхнему регистру.
    """
    if not s:
        return ""
    s = s.strip()

    repl_map = {
        "А": "A", "а": "a",
        "В": "B", "в": "b",
        "С": "C", "с": "c",
        "Е": "E", "е": "e",
        "К": "K", "к": "k",
        "М": "M", "м": "m",
        "Н": "H", "н": "h",
        "О": "O", "о": "o",
        "Р": "P", "р": "p",
        "Т": "T", "т": "t",
        "Х": "X", "х": "x",
    }

    s_norm = []
    for ch in s:
        s_norm.append(repl_map.get(ch, ch))

    return "".join(s_norm).upper()


def _articles_equal(a: str | None, b: str | None) -> bool:
    return _normalize_article(a) == _normalize_article(b)


# ==========================
# ПОИСК В product / bundle
# ==========================

def _find_in_entity_by_article(entity_url: str, article: str) -> dict | None:
    """
    Ищем в /entity/product или /entity/bundle по article.
    Учитываем ТОЛЬКО field 'article', code игнорируем.
    """
    target = article

    # 1. filter=article=...
    try:
        params = {"filter": f"article={target}"}
        data = _ms_get(entity_url, params=params)
        rows = data.get("rows", [])
        if rows:
            exact = [r for r in rows if _articles_equal(r.get("article"), target)]
            if exact:
                r0 = exact[0]
                print(
                    f"[MS] Найден в {entity_url} по article={target}: "
                    f"{r0.get('name')} (article={r0.get('article')})"
                )
                return r0
    except Exception as e:
        print(f"[MS] Ошибка filter article={target} в {entity_url}: {e!r}")

    # 2. search=...
    try:
        params = {"search": target}
        data = _ms_get(entity_url, params=params)
        rows = data.get("rows", [])
        if rows:
            exact = [r for r in rows if _articles_equal(r.get("article"), target)]
            if exact:
                r0 = exact[0]
                print(
                    f"[MS] Найден в {entity_url} по search={target} (точный article): "
                    f"{r0.get('name')} (article={r0.get('article')})"
                )
                return r0
    except Exception as e:
        print(f"[MS] Ошибка search={target} в {entity_url}: {e!r}")

    return None


def find_product_by_article(article: str) -> dict | None:
    """
    Ищет ассортименты в МойСклад по артикулу из Ozon.

      - Сначала /entity/product
      - Потом /entity/bundle
      - Сравнение только по article с нормализацией (E/Е и т.п.)
    """
    product_url = f"{MS_BASE_URL}/entity/product"
    bundle_url = f"{MS_BASE_URL}/entity/bundle"

    product = _find_in_entity_by_article(product_url, article)
    if product is not None:
        return product

    bundle = _find_in_entity_by_article(bundle_url, article)
    if bundle is not None:
        return bundle

    print(
        f"[MS] Не найдено ни product, ни bundle по article={article} "
        f"(после нормализации)"
    )
    return None


# ==========================
# ЗАКАЗЫ ПОКУПАТЕЛЯ
# ==========================

def create_customer_order(payload: dict) -> dict:
    """
    Создать заказ покупателя.
    Логируем только кратко: имя заказа и кол-во позиций.
    """
    url = f"{BASE_URL}/entity/customerorder"
    name = payload.get("name")
    positions = payload.get("positions") or []
    print(f"[MS] Создание заказа {name}, позиций: {len(positions)}")

    r = requests.post(url, json=payload, headers=HEADERS, timeout=30)
    if r.status_code >= 400:
        print(f"[MS ERROR] create_customer_order {name} status={r.status_code} body={r.text[:500]}")
    r.raise_for_status()
    return r.json()


def find_customer_order_by_name(name: str) -> dict | None:
    url = f"{BASE_URL}/entity/customerorder"
    params = {"filter": f"name={name}", "limit": 1}
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    if r.status_code >= 400:
        print(f"[MS ERROR] find_customer_order_by_name {name} status={r.status_code} body={r.text[:500]}")
    r.raise_for_status()
    data = r.json()
    rows = data.get("rows", [])
    return rows[0] if rows else None


def update_customer_order_state(order_meta_href: str, state_meta_href: str) -> dict:
    """
    Смена статуса заказа покупателя.
    В meta для state ОБЯЗАТЕЛЕН type="state".
    """
    # Получаем текущий заказ
    r = requests.get(order_meta_href, headers=HEADERS, timeout=30)
    if r.status_code >= 400:
        print(f"[MS ERROR] get order before state change status={r.status_code} body={r.text[:500]}")
    r.raise_for_status()
    order = r.json()

    # Обновляем поле state с корректной meta
    order["state"] = {
        "meta": {
            "href": state_meta_href,
            "type": "state",
            "mediaType": "application/json",
        }
    }

    # Сохраняем заказ
    r_put = requests.put(order_meta_href, headers=HEADERS, json=order, timeout=30)
    if r_put.status_code >= 400:
        print(f"[MS ERROR] update_customer_order_state status={r_put.status_code} body={r_put.text[:500]}")
    r_put.raise_for_status()
    return r_put.json()


def clear_reserve_for_order(order_meta_href: str) -> dict:
    """
    Снять резерв по всем позициям заказа покупателя.
    """
    # 1. Читаем сам заказ
    r = requests.get(order_meta_href, headers=HEADERS, timeout=30)
    r.raise_for_status()
    order = r.json()

    # 2. Получаем href позиций
    pos_meta = order.get("positions", {}).get("meta", {}).get("href")
    if not pos_meta:
        print("[MS] Нет позиций для снятия резерва")
        return order

    # 3. Загружаем позиции заказа
    r_pos = requests.get(pos_meta, headers=HEADERS, timeout=30)
    r_pos.raise_for_status()
    pos_list = r_pos.json().get("rows", [])

    # 4. Обнуляем reserve в каждой позиции
    for pos in pos_list:
        pos["reserve"] = 0

        # отправляем PUT для каждой позиции
        pos_href = pos["meta"]["href"]
        r_put = requests.put(pos_href, headers=HEADERS, json=pos, timeout=30)
        if r_put.status_code >= 400:
            print(f"[MS ERROR] clear_reserve pos PUT status={r_put.status_code} body={r_put.text[:300]}")
        r_put.raise_for_status()

    # 5. Возвращаем результат
    print(f"[MS] Резерв снят, позиций обработано: {len(pos_list)}")
    return order


def create_demand_from_order(order_meta_href: str) -> dict:
    """
    Создать Отгрузку (demand) на основе заказа покупателя.
    """
    # 1. Читаем заказ
    r = requests.get(order_meta_href, headers=HEADERS, timeout=30)
    r.raise_for_status()
    order = r.json()

    # 2. Получаем meta ссылку на позиции
    pos_meta_href = order.get("positions", {}).get("meta", {}).get("href")
    if not pos_meta_href:
        print("[MS ERROR] Нет позиций в заказе — не можем создать отгрузку")
        return {}

    # 3. Загружаем позиции заказа
    r_pos = requests.get(pos_meta_href, headers=HEADERS, timeout=30)
    r_pos.raise_for_status()
    positions = r_pos.json().get("rows", [])

    # 4. Формируем payload
    demand_payload = {
        "customerOrder": {"meta": order["meta"]},
        "organization": order.get("organization"),
        "agent": order.get("agent"),
        "store": order.get("store"),
        "positions": [],
    }

    for pos in positions:
        demand_payload["positions"].append(
            {
                "quantity": pos.get("quantity", 0),
                "assortment": pos.get("assortment"),
            }
        )

    # 5. Создаём отгрузку
    url = f"{BASE_URL}/entity/demand"
    r_post = requests.post(url, headers=HEADERS, json=demand_payload, timeout=30)
    if r_post.status_code >= 400:
        print(f"[MS ERROR] create_demand status={r_post.status_code} body={r_post.text[:500]}")
    r_post.raise_for_status()

    return r_post.json()
