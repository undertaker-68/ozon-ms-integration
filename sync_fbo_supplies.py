import os
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from ozon_fbo_client import OzonFboClient
from ms_client import (
    find_product_by_article,
    create_customer_order,
    find_customer_order_by_name,
    update_customer_order,
    MS_BASE_URL,
    HEADERS as MS_HEADERS,
)

try:
    from notifier import send_telegram_message
except Exception:  # noqa: BLE001
    def send_telegram_message(text: str) -> bool:  # type: ignore
        print("Telegram notifier не доступен:", text)
        return False


load_dotenv()

# -----------------------------
# НАСТРОЙКИ
# -----------------------------
DRY_RUN_FBO = os.getenv("DRY_RUN_FBO", "false").lower() == "true"

FBO_CUTOFF_FILE = "fbo_cutoff.json"

# Эти 3 заявки всегда в работе (даже если старые)
KEEP_ORDER_NUMBERS = {
    "2000037619561",
    "2000037545485",
    "2000037485754",
}

# Если хочешь вручную игнорировать конкретные номера — добавляй сюда
IGNORE_ORDER_NUMBERS: set[str] = set()

# Склад-источник перемещения (как ты сказал)
MS_SOURCE_STORE_UUID = "7cdb9b20-9910-11ec-0a80-08670002d998"
MS_SOURCE_STORE_HREF = f"{MS_BASE_URL}/entity/store/{MS_SOURCE_STORE_UUID}"

# Склад FBO в МС (из .env)
MS_FBO_STORE_HREF = os.getenv("MS_FBO_STORE_HREF", "").strip()

# Контрагент/организация в МС (уже используются в проекте)
MS_ORGANIZATION_HREF = os.getenv("MS_ORGANIZATION_HREF", "").strip()
MS_AGENT_HREF = os.getenv("MS_AGENT_HREF", "").strip()

# Статус заказа покупателя "FBO" (из .env)
MS_STATE_FBO = os.getenv("MS_STATE_FBO", "").strip()

# Статусы документов "Поставка" (опционально, из .env)
MS_STATE_SUPPLY_MOVE = os.getenv("MS_STATE_SUPPLY_MOVE", "").strip()
MS_STATE_SUPPLY_DEMAND = os.getenv("MS_STATE_SUPPLY_DEMAND", "").strip()

# Состояния Ozon, которые считаем "подготовка" (заказ+перемещение создаём/обновляем)
PREP_STATES = {"DATA_FILLING", "READY_TO_SUPPLY"}

# Состояния Ozon, при которых создаём отгрузку (после успешного перемещения)
DEMAND_STATES = {"IN_TRANSIT", "ACCEPTANCE_AT_STORAGE_WAREHOUSE"}

# Состояния, которые вообще берём из Ozon (чтобы не тянуть CANCELLED и т.п.)
OZON_STATES_FILTER = [
    "CREATED",
    "READY_TO_SUPPLY",
    "IN_TRANSIT",
    "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
    "REPORTS_CONFIRMATION_AWAITING",
]

# -----------------------------
# ВСПОМОГАТЕЛЬНОЕ
# -----------------------------
def _tg(text: str) -> None:
    try:
        send_telegram_message(text)
    except Exception:  # noqa: BLE001
        pass


def _valid_ms_href(href: str) -> bool:
    return bool(href) and href.startswith("https://api.moysklad.ru/api/remap/1.2/")


def _parse_ozon_dt(s: Any) -> Optional[datetime]:
    if not s or not isinstance(s, str):
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _iso(dt: datetime) -> str:
    # МС нормально принимает ISO с таймзоной
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _load_cutoff() -> Optional[datetime]:
    if not os.path.exists(FBO_CUTOFF_FILE):
        return None
    try:
        with open(FBO_CUTOFF_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        ts = data.get("cutoff")
        return _parse_ozon_dt(ts)
    except Exception:
        return None


def _ensure_cutoff() -> datetime:
    cutoff = _load_cutoff()
    if cutoff:
        return cutoff
    cutoff = datetime.now(timezone.utc)
    try:
        with open(FBO_CUTOFF_FILE, "w", encoding="utf-8") as f:
            json.dump({"cutoff": _iso(cutoff)}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    print(f"[FBO] Установлена отсечка для новых поставок: {_iso(cutoff)}")
    return cutoff


def _ms_get(url: str, params: Optional[dict] = None) -> dict:
    r = requests.get(url, headers=MS_HEADERS, params=params, timeout=40)
    r.raise_for_status()
    return r.json()


def _ms_post(url: str, payload: dict) -> dict:
    r = requests.post(url, headers=MS_HEADERS, json=payload, timeout=60)
    if r.status_code >= 400:
        print(f"[MS POST ERROR] {url} status={r.status_code} body={r.text}")
    r.raise_for_status()
    return r.json()


def _ms_put(url: str, payload: dict) -> dict:
    r = requests.put(url, headers=MS_HEADERS, json=payload, timeout=60)
    if r.status_code >= 400:
        print(f"[MS PUT ERROR] {url} status={r.status_code} body={r.text}")
    r.raise_for_status()
    return r.json()


def _ms_find_one(entity: str, name: str) -> Optional[dict]:
    url = f"{MS_BASE_URL}/entity/{entity}"
    params = {"filter": f"name={name}", "limit": 1}
    data = _ms_get(url, params=params)
    rows = data.get("rows") or []
    return rows[0] if rows else None


def _ms_get_positions(entity: str, entity_href: str) -> List[dict]:
    # positions href: <entity_href>/positions
    url = f"{entity_href}/positions"
    data = _ms_get(url, params={"limit": 1000})
    return data.get("rows") or []


def _get_cluster(dest_name: str) -> str:
    # Минимальная логика: ПУШКИНО -> Москва и МО (как ты просил)
    n = (dest_name or "").upper()
    if "ПУШКИНО" in n:
        return "Москва и МО"
    if "МОСКВ" in n:
        return "Москва и МО"
    return "Москва и МО"  # можно расширить позже, пока безопасно и соответствует твоему примеру


def _build_comment(order_number: str, dest_name: str) -> str:
    cluster = _get_cluster(dest_name)
    # ВАЖНО: тут НЕТ Красноярска и вообще склада отправки
    return f"{order_number} - {cluster} - {dest_name}"


def _extract_dest_warehouse(order: dict) -> Tuple[str, Optional[str]]:
    """
    Возвращает: (dest_name, planned_date_iso_or_None)
    planned_date берём из supplies[0].storage_warehouse.arrival_date, если есть, иначе timeslot.from
    """
    supplies = order.get("supplies") or []
    if supplies and isinstance(supplies, list):
        sw = (supplies[0] or {}).get("storage_warehouse") or {}
        dest_name = str(sw.get("name") or "").strip()
        arrival = sw.get("arrival_date")
        if isinstance(arrival, str) and arrival:
            # Ozon отдаёт "YYYY-MM-DDT00:00:00Z" или "YYYY-MM-DDT00:00:00Z" / иногда без времени
            dt = _parse_ozon_dt(arrival) or _parse_ozon_dt(arrival + "T00:00:00Z")
            return (dest_name or "UNKNOWN_DEST", _iso(dt) if dt else None)

    # fallback: timeslot.from
    timeslot = (order.get("timeslot") or {}).get("timeslot") or {}
    frm = timeslot.get("from")
    dest_name = "UNKNOWN_DEST"
    dt = _parse_ozon_dt(frm) if isinstance(frm, str) else None
    return (dest_name, _iso(dt) if dt else None)


def _build_ms_positions_from_bundle_items(bundle_items: List[dict]) -> Tuple[List[dict], List[str]]:
    """
    bundle_items -> positions payload for MS order/move/demand.
    Важно: связка по offer_id (артикул), НЕ sku.
    Цена — из МС (salePrices[0].value).
    """
    errors: List[str] = []
    ms_positions: List[dict] = []

    for it in bundle_items:
        offer_id = it.get("offer_id")
        qty = it.get("quantity") or 0
        if not offer_id or qty <= 0:
            continue

        product = find_product_by_article(str(offer_id))
        if not product:
            errors.append(f"Товар с артикулом {offer_id!r} не найден в МойСклад")
            continue

        price = None
        sale_prices = product.get("salePrices")
        if isinstance(sale_prices, list) and sale_prices:
            first_price = sale_prices[0] or {}
            price = first_price.get("value")

        pos = {
            "quantity": qty,
            "assortment": {"meta": product["meta"]},
        }
        if price is not None:
            pos["price"] = price
        ms_positions.append(pos)

    return ms_positions, errors


def _build_ms_order_payload(order_number: str, comment: str, planned_iso: Optional[str], positions: List[dict]) -> dict:
    org_meta = {"href": MS_ORGANIZATION_HREF, "type": "organization", "mediaType": "application/json"}
    agent_meta = {"href": MS_AGENT_HREF, "type": "counterparty", "mediaType": "application/json"}
    store_meta = {"href": MS_FBO_STORE_HREF, "type": "store", "mediaType": "application/json"}

    payload = {
        "name": order_number,
        "organization": {"meta": org_meta},
        "agent": {"meta": agent_meta},
        "store": {"meta": store_meta},  # склад FBO
        "positions": positions,
        "description": comment,
    }

    # статус заказа FBO
    if _valid_ms_href(MS_STATE_FBO):
        payload["state"] = {"meta": {"href": MS_STATE_FBO, "type": "state", "mediaType": "application/json"}}

    # планируемая дата отгрузки
    if planned_iso:
        payload["deliveryPlannedMoment"] = planned_iso

    return payload


def _create_or_update_ms_order(order_number: str, payload: dict) -> dict:
    existing = find_customer_order_by_name(order_number)
    if existing:
        href = existing["meta"]["href"]
        updated = update_customer_order(href, payload)
        return updated
    created = create_customer_order(payload)
    return created


def _ensure_move(order_number: str, comment: str, order: dict) -> Optional[dict]:
    """
    Перемещение создаём/обновляем ВСЕГДА после заказа.
    Обновление делаем PUT (без удаления), чтобы при редактировании поставки в Ozon
    перемещение соответствовало актуальному составу.
    """
    if not _valid_ms_href(MS_SOURCE_STORE_HREF) or not _valid_ms_href(MS_FBO_STORE_HREF):
        _tg(f"❗ FBO {order_number}: не задан склад-источник или склад FBO в МС (MS_SOURCE_STORE_HREF/MS_FBO_STORE_HREF)")
        return None

    order_href = (order.get("meta") or {}).get("href")
    if not order_href:
        return None

    order_positions = _ms_get_positions("customerorder", order_href)
    if not order_positions:
        msg = f"❗ FBO {order_number}: в заказе МС нет позиций, перемещение не создаём."
        print(msg)
        _tg(msg)
        return None

    move_payload = {
        "name": order_number,
        "description": comment,
        "sourceStore": {"meta": {"href": MS_SOURCE_STORE_HREF, "type": "store", "mediaType": "application/json"}},
        "targetStore": {"meta": {"href": MS_FBO_STORE_HREF, "type": "store", "mediaType": "application/json"}},
        "positions": [
            {
                "quantity": p.get("quantity", 0),
                "assortment": p.get("assortment"),
                "price": p.get("price", 0),
            }
            for p in order_positions
            if p.get("assortment")
        ],
    }

    if _valid_ms_href(MS_STATE_SUPPLY_MOVE):
        move_payload["state"] = {"meta": {"href": MS_STATE_SUPPLY_MOVE, "type": "state", "mediaType": "application/json"}}

    existing_move = _ms_find_one("move", order_number)
    try:
        if existing_move:
            href = existing_move["meta"]["href"]
            print(f"[FBO] Обновляем перемещение {order_number}")
            return _ms_put(href, move_payload)
        print(f"[FBO] Создаём перемещение {order_number} (СКЛАД → FBO)")
        return _ms_post(f"{MS_BASE_URL}/entity/move", move_payload)
    except requests.HTTPError as e:
        txt = f"❗ FBO {order_number}: не удалось создать/обновить перемещение: {e!r}"
        print(txt)
        _tg(txt)
        return None


def _ensure_demand(order_number: str, comment: str, order: dict) -> Optional[dict]:
    """
    Отгрузка 1 на заявку.
    Создаём только если её ещё нет.
    """
    existing_demand = _ms_find_one("demand", order_number)
    if existing_demand:
        return existing_demand

    order_href = (order.get("meta") or {}).get("href")
    if not order_href:
        return None

    order_positions = _ms_get_positions("customerorder", order_href)
    if not order_positions:
        msg = f"❗ FBO {order_number}: в заказе МС нет позиций, отгрузку не создаём."
        print(msg)
        _tg(msg)
        return None

    demand_payload = {
        "name": order_number,
        "description": comment,
        "customerOrder": {"meta": order["meta"]},
        "organization": order["organization"],
        "agent": order["agent"],
        "store": order["store"],
        "positions": [
            {
                "quantity": p.get("quantity", 0),
                "assortment": p.get("assortment"),
                "price": p.get("price", 0),
            }
            for p in order_positions
            if p.get("assortment")
        ],
    }

    # статус отгрузки "Поставка" — опционально, но не должен ронять скрипт
    if _valid_ms_href(MS_STATE_SUPPLY_DEMAND):
        demand_payload["state"] = {"meta": {"href": MS_STATE_SUPPLY_DEMAND, "type": "state", "mediaType": "application/json"}}

    try:
        print(f"[FBO] Создаём отгрузку {order_number} (1 на заявку)")
        return _ms_post(f"{MS_BASE_URL}/entity/demand", demand_payload)
    except requests.HTTPError as e:
        txt = f"❗ FBO {order_number}: не удалось создать отгрузку: {e!r}"
        print(txt)
        _tg(txt)
        return None


def _should_process(order: dict, cutoff: Optional[datetime]) -> bool:
    """
    Логика:
      - KEEP -> всегда
      - IGNORE -> никогда
      - остальные -> только если ref_dt >= cutoff
        ref_dt = state_updated_date || created_date
    """
    order_number = str(order.get("order_number") or order.get("order_id") or "")

    if order_number in IGNORE_ORDER_NUMBERS:
        return False

    if order_number in KEEP_ORDER_NUMBERS:
        return True

    if not cutoff:
        return False

    created_dt = _parse_ozon_dt(order.get("created_date"))
    updated_dt = _parse_ozon_dt(order.get("state_updated_date"))
    ref_dt = updated_dt or created_dt

    if not ref_dt:
        return False
    return ref_dt >= cutoff


def _collect_bundle_positions(order: dict, client: OzonFboClient) -> Tuple[List[dict], List[str]]:
    """
    В Ozon детали заявки содержат supplies[].bundle_id — по нему берём товары.
    """
    errors: List[str] = []
    supplies = order.get("supplies") or []
    if not supplies or not isinstance(supplies, list):
        return [], ["Нет supplies в заявке Ozon"]

    # Берём все bundle_id (в реальности часто 1, но бывает несколько)
    bundle_ids = []
    for s in supplies:
        bid = (s or {}).get("bundle_id")
        if isinstance(bid, str) and bid:
            bundle_ids.append(bid)

    if not bundle_ids:
        return [], ["Нет bundle_id в supplies"]

    all_positions: List[dict] = []
    for bid in bundle_ids:
        try:
            items = client.get_bundle_items(bid)
            ms_pos, errs = _build_ms_positions_from_bundle_items(items)
            errors.extend([f"{e} (bundle_id={bid})" for e in errs])
            all_positions.extend(ms_pos)
            # небольшой анти-429
            time.sleep(0.15)
        except requests.HTTPError as e:
            errors.append(f"Ошибка получения bundle {bid}: {e!r}")
        except Exception as e:  # noqa: BLE001
            errors.append(f"Ошибка bundle {bid}: {e!r}")

    # Склеиваем одинаковые assortments (если один товар пришёл из разных bundle)
    merged: Dict[str, dict] = {}
    for p in all_positions:
        ahref = (((p.get("assortment") or {}).get("meta") or {}).get("href")) or ""
        if not ahref:
            continue
        if ahref not in merged:
            merged[ahref] = dict(p)
        else:
            merged[ahref]["quantity"] = (merged[ahref].get("quantity") or 0) + (p.get("quantity") or 0)

    return list(merged.values()), errors


def _process_single(order: dict, client: OzonFboClient, cutoff: datetime) -> None:
    order_number = str(order.get("order_number") or order.get("order_id") or "")
    oz_state = str(order.get("state") or "").upper()

    if not order_number:
        return

    if not _should_process(order, cutoff):
        return

    # Берём склад назначения и планируемую дату
    dest_name, planned_iso = _extract_dest_warehouse(order)
    comment = _build_comment(order_number, dest_name)

    # Собираем позиции
    positions, pos_errors = _collect_bundle_positions(order, client)
    if pos_errors:
        for e in pos_errors[:5]:
            print(f"[FBO] {order_number}: {e}")
    if not positions:
        msg = f"❗ FBO {order_number}: не удалось подобрать позиции МС по поставке (нет товаров по артикулам)."
        print(msg)
        _tg(msg)
        return

    # Заказ в МС (создать/обновить)
    payload = _build_ms_order_payload(order_number, comment, planned_iso, positions)

    print(f"[FBO] Обработка заявки {order_number} (аккаунт={client.account_name}, state={oz_state}), позиций={len(positions)}, DRY_RUN={DRY_RUN_FBO}")

    if DRY_RUN_FBO:
        return

    ms_order = _create_or_update_ms_order(order_number, payload)

    # Перемещение — всегда после заказа (и обновляется при редактировании)
    move = _ensure_move(order_number, comment, ms_order)
    if move is None:
        # перемещение не создали — уведомление уже ушло, дальше не идём (и отгрузку не создаём)
        return

    # Отгрузка — только при нужном статусе
    if oz_state in DEMAND_STATES:
        demand = _ensure_demand(order_number, comment, ms_order)
        if demand is None:
            # demand не создали — уведомление уже ушло
            return
        _tg(f"✅ FBO: создана/есть отгрузка по заявке №{order_number} на склад {dest_name} ({client.account_name})")


def sync_fbo_supplies(limit: int = 50, days_back: int = 30) -> None:
    print(f"Запуск синхронизации FBO-поставок (limit={limit}, days_back={days_back}, DRY_RUN={DRY_RUN_FBO})")
    cutoff = _ensure_cutoff()
    print(f"[FBO] Текущая отсечка: {_iso(cutoff)}")

    accounts = [
        ("ozon1", os.getenv("OZON_CLIENT_ID", ""), os.getenv("OZON_API_KEY", "")),
        ("ozon2", os.getenv("OZON2_CLIENT_ID", ""), os.getenv("OZON2_API_KEY", "")),
    ]

    for acc, cid, key in accounts:
        cid = (cid or "").strip()
        key = (key or "").strip()
        if not cid or not key:
            print(f"[FBO] Пропуск кабинета {acc}: нет ключей")
            continue

        client = OzonFboClient(client_id=cid, api_key=key, account_name=acc)

        print(f"[OZON FBO] Запрос списка заявок на поставку ({acc}), limit={limit}, days_back={days_back}")
        ids = client.list_supply_order_ids(limit=limit, days_back=days_back, states=OZON_STATES_FILTER)
        print(f"[OZON FBO] Получено заявок на поставку (IDs) ({acc}): {len(ids)}")

        if not ids:
            continue

        print(f"[OZON FBO] Получение деталей заявок (get) ({acc}), ids={ids}")
        orders = client.get_supply_orders_by_ids(ids)
        print(f"[OZON FBO] Всего заявок с деталями ({acc}): {len(orders)}")
        print(f"[FBO] Кабинет {acc}: получено заявок: {len(orders)}")

        for order in orders:
            try:
                _process_single(order, client, cutoff)
            except Exception as e:  # noqa: BLE001
                num = str(order.get("order_number") or order.get("order_id") or "UNKNOWN")
                msg = f"❗ FBO {num}: ошибка обработки ({acc}): {e!r}"
                print(msg)
                _tg(msg)


if __name__ == "__main__":
    sync_fbo_supplies(limit=50, days_back=30)
