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
MS_BASE_URL = BASE_URL  # для совместимости

auth_str = f"{MS_LOGIN}:{MS_PASSWORD}"
auth_bytes = auth_str.encode("utf-8")
auth_b64 = base64.b64encode(auth_bytes).decode("utf-8")

HEADERS = {
    "Authorization": f"Basic {auth_b64}",
    "Content-Type": "application/json",
    "Accept-Encoding": "gzip",
    "User-Agent": "ozon-ms-integration-bot",
}


def _get_headers():
    return HEADERS


def _ms_get(path: str, params=None):
    """
    Универсальный GET к МойСклад.

    path:
      - либо полный URL (https://api.moysklad.ru/...)
      - либо относительный путь (начиная с /entity/...)
    """
    if path.startswith("http://") or path.startswith("https://"):
        url = path
    else:
        if not path.startswith("/"):
            path = "/" + path
        url = f"{MS_BASE_URL}{path}"

    r = requests.get(url, headers=_get_headers(), params=params, timeout=30)
    if r.status_code >= 400:
        print(f"[MS GET ERROR] {url} status={r.status_code} body={r.text}")
    r.raise_for_status()
    return r.json()


def _ms_post(path: str, json_data: dict):
    if path.startswith("http://") or path.startswith("https://"):
        url = path
    else:
        if not path.startswith("/"):
            path = "/" + path
        url = f"{MS_BASE_URL}{path}"

    r = requests.post(url, headers=_get_headers(), json=json_data, timeout=30)
    if r.status_code >= 400:
        print(f"[MS POST ERROR] {url} status={r.status_code} body={r.text[:500]}")
    r.raise_for_status()
    return r.json()


# =====================================================
#  КЛИЕНТЫ, КОНТРАГЕНТЫ, ОРГАНИЗАЦИИ, СКЛАДЫ и т.п.
# =====================================================

def get_organization_by_name(name: str):
    params = {"filter": f'name={name}', "limit": 1}
    data = _ms_get("/entity/organization", params=params)
    rows = data.get("rows", [])
    return rows[0] if rows else None


def get_store_by_name(name: str):
    params = {"filter": f'name={name}', "limit": 1}
    data = _ms_get("/entity/store", params=params)
    rows = data.get("rows", [])
    return rows[0] if rows else None


def get_counterparty_by_name_or_inn(name: str, inn: str | None = None):
    """
    Поиск контрагента по наименованию и/или ИНН.
    """
    filters = []
    if name:
        filters.append(f'name~{name}')
    if inn:
        filters.append(f'inn={inn}')
    if not filters:
        return None

    params = {"filter": ";".join(filters), "limit": 1}
    data = _ms_get("/entity/counterparty", params=params)
    rows = data.get("rows", [])
    return rows[0] if rows else None


def create_counterparty(name: str, inn: str | None = None, phone: str | None = None, email: str | None = None):
    payload = {
        "name": name,
        "companyType": "individual",
    }
    if inn:
        payload["inn"] = inn
    if phone:
        payload["phone"] = phone
    if email:
        payload["email"] = email

    return _ms_post("/entity/counterparty", payload)


def get_or_create_counterparty(name: str, inn: str | None = None, phone: str | None = None, email: str | None = None):
    cp = get_counterparty_by_name_or_inn(name, inn=inn)
    if cp:
        return cp
    return create_counterparty(name=name, inn=inn, phone=phone, email=email)


def get_ms_product_by_code(code: str):
    """
    Поиск товара/услуги/комплекта по артикулу (code).
    """
    params = {"filter": f"code={code}", "limit": 1}
    data = _ms_get("/entity/assortment", params=params)
    rows = data.get("rows", [])
    return rows[0] if rows else None


def get_sales_channel_by_id(channel_id: str):
    return _ms_get(f"/entity/saleschannel/{channel_id}")


# ==============================================
#  ЗАКАЗЫ ПОКУПАТЕЛЯ (customerorder)
# ==============================================

def find_customer_order_by_name(name: str):
    """
    Ищем заказ покупателя в МойСклад по точному name.
    Возвращаем первый найденный объект или None.
    """
    params = {
        "filter": f"name={name}",
        "limit": 1,
        "expand": "positions",
    }
    data = _ms_get("/entity/customerorder", params=params)
    rows = data.get("rows", [])
    return rows[0] if rows else None


def create_customer_order(
    organization: dict,
    agent: dict,
    store: dict,
    name: str,
    positions: list[dict],
    description: str | None = None,
):
    """
    Создать заказ покупателя в МойСклад.
    """
    payload = {
        "name": name,
        "organization": {"meta": organization["meta"]},
        "agent": {"meta": agent["meta"]},
        "store": {"meta": store["meta"]},
        "positions": [],
    }

    if description:
        payload["description"] = description

    for pos in positions:
        assortment = pos.get("assortment")
        quantity = pos.get("quantity", 0)
        price = pos.get("price")
        reserve = pos.get("reserve")

        if not isinstance(assortment, dict):
            continue
        if not quantity or quantity <= 0:
            continue

        line = {
            "assortment": {"meta": assortment["meta"]},
            "quantity": quantity,
        }
        if price is not None:
            line["price"] = price
        if reserve is not None:
            line["reserve"] = reserve

        payload["positions"].append(line)

    print(f"[MS] Создание заказа покупателя {name}")
    return _ms_post("/entity/customerorder", payload)


def update_customer_order_positions(order_href: str, positions: list[dict]):
    """
    Перезапись позиций существующего заказа покупателя.
    """
    order = _ms_get(order_href)
    payload = {
        "name": order.get("name"),
        "organization": order.get("organization"),
        "agent": order.get("agent"),
        "store": order.get("store"),
        "positions": [],
    }

    for pos in positions:
        assortment = pos.get("assortment")
        quantity = pos.get("quantity", 0)
        price = pos.get("price")
        reserve = pos.get("reserve")

        if not isinstance(assortment, dict):
            continue
        if not quantity or quantity <= 0:
            continue

        line = {
            "assortment": {"meta": assortment["meta"]},
            "quantity": quantity,
        }
        if price is not None:
            line["price"] = price
        if reserve is not None:
            line["reserve"] = reserve

        payload["positions"].append(line)

    print(f"[MS] Обновление позиций заказа покупателя {order.get('name')}")
    r = requests.put(
        order_href,
        headers=_get_headers(),
        json=payload,
        timeout=30,
    )
    if r.status_code >= 400:
        print(f"[MS ERROR] update_customer_order_positions status={r.status_code} body={r.text[:500]}")
    r.raise_for_status()
    return r.json()


def get_customer_order_positions(order_href: str):
    """
    Возвращает список позиций заказа покупателя.
    """
    url = order_href.split("?")[0] + "/positions"
    data = _ms_get(url)
    return data.get("rows", [])


def get_demand_by_customer_order(order_href: str):
    """
    Ищет отгрузку (demand), связанную с данным заказом покупателя.
    Возвращает первый найденный объект или None.
    """
    uuid = get_ms_uuid_from_meta_href(order_href)
    params = {
        "filter": f"customerOrder=https://api.moysklad.ru/api/remap/1.2/entity/customerorder/{uuid}",
        "limit": 1,
    }
    data = _ms_get("/entity/demand", params=params)
    rows = data.get("rows", [])
    return rows[0] if rows else None


def create_demand_from_order(order_or_href):
    """
    Создаёт отгрузку (demand) по заказу покупателя.

    :param order_or_href: либо объект заказа (dict) из МойСклад, либо meta.href (str) этого заказа.
    :return: созданный объект отгрузки (dict)
    """
    # Определяем href заказа
    if isinstance(order_or_href, str):
        order_href = order_or_href
    elif isinstance(order_or_href, dict):
        order_href = order_or_href.get("meta", {}).get("href")
    else:
        raise TypeError(f"Unsupported type for order_or_href: {type(order_or_href)}")

    if not order_href:
        raise ValueError("Не удалось определить href заказа покупателя для создания отгрузки")

    # Всегда запрашиваем заказ с expand=positions, чтобы получить строки
    base_href = order_href.split("?")[0]
    order_url = base_href + "?expand=positions"

    print(f"[MS] Загрузка заказа покупателя для отгрузки: {order_url}")
    order = _ms_get(order_url)

    # Базовый объект отгрузки
    demand = {
        "customerOrder": {"meta": order["meta"]},
    }

    # Копируем основные поля из заказа, если они есть
    for key in (
        "organization",
        "agent",
        "store",
        "group",
        "owner",
        "project",
        "state",
        "rate",
        "attributes",
        "vatEnabled",
        "vatIncluded",
        "vatSum",
        "applicable",
        "description",
    ):
        if key in order and order.get(key) is not None:
            demand[key] = order[key]

    # Дата документа – как у заказа, если есть
    if "moment" in order:
        demand["moment"] = order["moment"]

    # Строки отгрузки на основе строк заказа
    positions_payload = []

    positions_meta = order.get("positions")
    if isinstance(positions_meta, dict):
        rows = positions_meta.get("rows", [])
    else:
        rows = []

    for pos in rows:
        assortment = pos.get("assortment")
        quantity = pos.get("quantity", 0)

        if not isinstance(assortment, dict):
            continue
        if not quantity or quantity <= 0:
            continue

        line = {
            "assortment": assortment,
            "quantity": quantity,
        }

        # По возможности переносим цену / резерв
        if "price" in pos:
            line["price"] = pos["price"]
        if "reserve" in pos:
            line["reserve"] = pos["reserve"]

        positions_payload.append(line)

    if not positions_payload:
        print("[MS WARNING] Для заказа нет строк позиций при создании отгрузки")

    demand["positions"] = positions_payload

    print("[MS] Создание отгрузки в МойСklad через /entity/demand")
    r_post = requests.post(
        f"{MS_BASE_URL}/entity/demand",
        headers=_get_headers(),
        json=demand,
        timeout=30,
    )
    if r_post.status_code >= 400:
        print(
            f"[MS ERROR] create_demand status={r_post.status_code} body={r_post.text[:500]}"
        )
    r_post.raise_for_status()
    return r_post.json()


def get_ms_uuid_from_meta_href(href: str) -> str:
    """
    Из meta.href вида
      https://api.moysklad.ru/api/remap/1.2/entity/customerorder/30fdc0b9-d045-11f0-0a80-140f001d4336
    вытаскиваем только UUID (последний сегмент).
    """
    return href.rstrip("/").split("/")[-1]
