import base64
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

# Загружаем переменные окружения из .env
load_dotenv()

logger = logging.getLogger(__name__)

MS_LOGIN = os.getenv("MS_LOGIN")
MS_PASSWORD = os.getenv("MS_PASSWORD")
MS_PROJECT_OZON_FBS = os.getenv("MS_PROJECT_OZON_FBS")  # href проекта для Ozon FBS
MS_OZON_STORE_HREF = os.getenv("MS_OZON_STORE_HREF")  # href склада, который используем как дефолт
MS_ORGANIZATION_HREF = os.getenv("MS_ORGANIZATION_HREF")  # href юр.лица
MS_AGENT_HREF = os.getenv("MS_AGENT_HREF")  # href контрагента Ozon
MS_CUSTOMER_ORDER_STATE_NEW = os.getenv("MS_CUSTOMER_ORDER_STATE_NEW")  # href статуса "Новый заказ"
MS_CUSTOMER_ORDER_STATE_IN_WORK = os.getenv("MS_CUSTOMER_ORDER_STATE_IN_WORK")  # href статуса "В работе"
MS_CUSTOMER_ORDER_STATE_DONE = os.getenv("MS_CUSTOMER_ORDER_STATE_DONE")  # href статуса "Выполнен"
MS_ATTR_OZON_POSTING_NUMBER = os.getenv("MS_ATTR_OZON_POSTING_NUMBER")  # href атрибута "Номер отправления Ozon"
MS_ATTR_DELIVERY_DATE = os.getenv("MS_ATTR_DELIVERY_DATE")  # href атрибута "Дата отгрузки"
MS_ATTR_DELIVERY_METHOD = os.getenv("MS_ATTR_DELIVERY_METHOD")  # href атрибута "Способ доставки"
MS_ATTR_WAREHOUSE_FROM = os.getenv("MS_ATTR_WAREHOUSE_FROM")  # href атрибута "Склад отгрузки"
MS_ATTR_DELIVERY_SERVICE = os.getenv("MS_ATTR_DELIVERY_SERVICE")  # href атрибута "Служба доставки"
MS_ATTR_OZON_CITY = os.getenv("MS_ATTR_OZON_CITY")  # href атрибута "Город доставки"
MS_ATTR_RECIPIENT_PHONE = os.getenv("MS_ATTR_RECIPIENT_PHONE")  # href атрибута "Телефон получателя"
MS_ATTR_RECIPIENT_NAME = os.getenv("MS_ATTR_RECIPIENT_NAME")  # href атрибута "Имя получателя"
MS_ATTR_POSTAMAT_ADDRESS = os.getenv("MS_ATTR_POSTAMAT_ADDRESS")  # href атрибута "Адрес ПВЗ/Постамат"
MS_ATTR_TRACKING_NUMBER = os.getenv("MS_ATTR_TRACKING_NUMBER")  # href атрибута "Трек-номер"
MS_ATTR_OZON_DISCOUNT = os.getenv("MS_ATTR_OZON_DISCOUNT")  # href атрибута "Скидка Ozon"

BASE_URL = "https://api.moysklad.ru/api/remap/1.2"

# Базовая авторизация
if not MS_LOGIN or not MS_PASSWORD:
    raise RuntimeError("Не заданы MS_LOGIN / MS_PASSWORD в .env")

auth_str = f"{MS_LOGIN}:{MS_PASSWORD}"
auth_b64 = base64.b64encode(auth_str.encode("utf-8")).decode("utf-8")

HEADERS = {
    "Authorization": f"Basic {auth_b64}",
    "Content-Type": "application/json",
    "Accept-Encoding": "gzip",
}


class MsClientError(Exception):
    """Базовое исключение для ошибок работы с МойСклад."""


class MsRateLimitError(MsClientError):
    """Превышено ограничение на количество запросов."""


def _handle_ms_error(resp: requests.Response, url: str) -> None:
    """Общий разбор ошибок МойСклад."""
    text = resp.text
    try:
        data = resp.json()
    except Exception:
        data = None

    if resp.status_code == 429:
        logger.error(
            "[MS GET ERROR] %s status=%s body=%s", url, resp.status_code, text
        )
        raise MsRateLimitError(
            f"МойСклад: Превышено ограничение на количество запросов "
            f"(code=429, url={url})"
        )

    logger.error("[MS ERROR] %s status=%s body=%s", url, resp.status_code, text)
    raise MsClientError(f"МойСклад вернул ошибку {resp.status_code} по url={url}")


def _ms_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """GET-запрос к МойСклад (относительный путь)."""
    url = f"{BASE_URL}{path}"
    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    if resp.status_code >= 400:
        _handle_ms_error(resp, url)
    return resp.json()


def _ms_post(path: str, json: Dict[str, Any]) -> Dict[str, Any]:
    """POST-запрос к МойСклад (относительный путь)."""
    url = f"{BASE_URL}{path}"
    resp = requests.post(url, headers=HEADERS, json=json, timeout=30)
    if resp.status_code >= 400:
        _handle_ms_error(resp, url)
    return resp.json()


def _ms_put(path: str, json: Dict[str, Any]) -> Dict[str, Any]:
    """PUT-запрос к МойСклад (относительный путь)."""
    url = f"{BASE_URL}{path}"
    resp = requests.put(url, headers=HEADERS, json=json, timeout=30)
    if resp.status_code >= 400:
        _handle_ms_error(resp, url)
    return resp.json()


def _ms_get_by_href(href: str) -> Dict[str, Any]:
    """GET по абсолютному href МойСклад."""
    resp = requests.get(href, headers=HEADERS, timeout=30)
    if resp.status_code >= 400:
        _handle_ms_error(resp, href)
    return resp.json()


# ---------------------------------------------------------------------------
# Ассортимент / товары / комплекты
# ---------------------------------------------------------------------------


def get_stock_all(
    limit: int = 1000, offset: int = 0, store_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Возвращает отчёт по остаткам (report/stock/all) с постраничной выборкой.

    :param limit: размер страницы
    :param offset: смещение
    :param store_id: id склада (UUID без /entity/store/)
    """
    params: Dict[str, Any] = {
        "limit": limit,
        "offset": offset,
        "expand": "assortment",
    }

    if store_id:
        params["stockStore"] = f"https://api.moysklad.ru/api/remap/1.2/entity/store/{store_id}"
    elif MS_OZON_STORE_HREF:
        params["stockStore"] = MS_OZON_STORE_HREF

    url_path = "/report/stock/all"
    url = f"{BASE_URL}{url_path}"
    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    if resp.status_code >= 400:
        _handle_ms_error(resp, url)
    return resp.json()


def get_assortment(
    limit: int = 1000,
    offset: int = 0,
    filters: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Возвращает список ассортимента /entity/assortment с фильтрами.
    """
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    if filters:
        filter_parts = [f"{k}={v}" for k, v in filters.items()]
        params["filter"] = ";".join(filter_parts)

    return _ms_get("/entity/assortment", params=params)


def find_product_by_article(article: str) -> Optional[Dict[str, Any]]:
    """
    Ищет товар/услугу/комплект по артикулу (code).
    Возвращает первую найденную позицию или None.
    """
    params = {
        "filter": f"code={article}",
        "limit": 1,
    }
    data = _ms_get("/entity/assortment", params=params)
    rows = data.get("rows", [])
    return rows[0] if rows else None


def get_bundle_components(bundle_href: str) -> List[Dict[str, Any]]:
    """
    Для комплекта (bundle) возвращает список компонент (positions).
    """
    bundle = _ms_get_by_href(bundle_href)
    positions = bundle.get("components") or []
    return positions


def compute_bundle_available(bundle: Dict[str, Any], stock_by_href: Dict[str, int]) -> int:
    """
    Считает доступное количество комплекта исходя из остатков по его компонентам.

    :param bundle: объект комплекта из отчёта/ассортимента
    :param stock_by_href: словарь {href номенклатуры -> доступный остаток}
    :return: максимальное целое количество комплекта
    """
    components = bundle.get("components") or []
    if not components:
        return 0

    available_list: List[int] = []

    for comp in components:
        assortment = comp.get("assortment") or {}
        comp_href = assortment.get("meta", {}).get("href")
        if not comp_href:
            continue

        quantity = comp.get("quantity") or 1
        total_stock = stock_by_href.get(comp_href, 0)

        # сколько комплектов можно собрать по этой компоненте
        if quantity <= 0:
            continue
        available_for_comp = total_stock // quantity
        available_list.append(available_for_comp)

    if not available_list:
        return 0

    return min(available_list)


# ---------------------------------------------------------------------------
# Работа с заказами покупателей
# ---------------------------------------------------------------------------


def create_customer_order(
    name: str,
    organization_href: str,
    agent_href: str,
    store_href: str,
    project_href: Optional[str],
    positions: List[Dict[str, Any]],
    attributes: List[Dict[str, Any]],
    state_href: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Создаёт заказ покупателя в МойСклад.
    """
    payload: Dict[str, Any] = {
        "name": name,
        "organization": {"meta": {"href": organization_href}},
        "agent": {"meta": {"href": agent_href}},
        "store": {"meta": {"href": store_href}},
        "positions": positions,
        "attributes": attributes,
    }

    if project_href:
        payload["project"] = {"meta": {"href": project_href}}

    if state_href:
        payload["state"] = {"meta": {"href": state_href}}

    logger.info(
        "[MS] Создаём заказ покупателя name=%s, store=%s, project=%s",
        name,
        store_href,
        project_href,
    )

    return _ms_post("/entity/customerorder", json=payload)


def find_customer_order_by_name(name: str) -> Optional[Dict[str, Any]]:
    """
    Ищет заказ покупателя по имени (точное совпадение).
    """
    params = {
        "filter": f"name={name}",
        "limit": 1,
    }
    data = _ms_get("/entity/customerorder", params=params)
    rows = data.get("rows", [])
    return rows[0] if rows else None


def update_customer_order_state(order: Dict[str, Any], state_href: str) -> Dict[str, Any]:
    """
    Обновляет статус (state) заказа покупателя.
    """
    order_href = order.get("meta", {}).get("href")
    if not order_href:
        raise MsClientError("У заказа нет meta.href")

    payload = {
        "state": {
            "meta": {
                "href": state_href,
            }
        }
    }

    # order_href уже полный URL вида https://api.moysklad.ru/api/remap/1.2/entity/customerorder/...
    return _ms_put(order_href.replace(BASE_URL, ""), json=payload)


def clear_reserve_for_order(order: Dict[str, Any]) -> Dict[str, Any]:
    """
    Сбрасывает резерв по заказу (reservedSum = 0).
    """
    order_href = order.get("meta", {}).get("href")
    if not order_href:
        raise MsClientError("У заказа нет meta.href")

    payload = {
        "reservedSum": 0,
    }

    return _ms_put(order_href.replace(BASE_URL, ""), json=payload)


def create_demand_from_order(order: Dict[str, Any]) -> Dict[str, Any]:
    """
    Создаёт отгрузку (demand) на основании заказа покупателя.

    Логика:
      1. Берём заказ (organization, agent, store, project, attributes, positions).
      2. Формируем payload для /entity/demand.
      3. Отправляем POST.
    """
    order_href = order.get("meta", {}).get("href")
    if not order_href:
        raise MsClientError("У заказа нет meta.href")

    org = order.get("organization") or {}
    agent = order.get("agent") or {}
    store = order.get("store") or {}
    project = order.get("project")
    attributes = order.get("attributes") or []
    positions = order.get("positions") or []

    demand_payload: Dict[str, Any] = {
        "organization": org,
        "agent": agent,
        "store": store,
        "customerOrder": {"meta": {"href": order_href}},
        "attributes": attributes,
        "positions": [],
    }

    if project:
        demand_payload["project"] = project

    # Копируем позиции из заказа
    for pos in positions:
        assortment = pos.get("assortment") or {}
        quantity = pos.get("quantity") or 0
        price = pos.get("price") or 0

        demand_payload["positions"].append(
            {
                "assortment": assortment,
                "quantity": quantity,
                "price": price,
            }
        )

    logger.info(
        "[MS] Создаём отгрузку из заказа id=%s name=%s",
        order.get("id"),
        order.get("name"),
    )

    # /entity/demand — стандартная точка создания отгрузки
    return _ms_post("/entity/demand", json=demand_payload)
