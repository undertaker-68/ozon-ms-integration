import os
import json
import time
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import base64
import requests
from dotenv import load_dotenv

from ozon_fbo_client import OzonFboClient
from ms_client import (
    find_product_by_article,
    create_customer_order,
    find_customer_order_by_name,
    update_customer_order,
)

from notifier import send_telegram_message

load_dotenv()

# ==========================
# НАСТРОЙКИ
# ==========================

DRY_RUN_FBO = os.getenv("DRY_RUN_FBO", "false").lower() == "true"

# Две текущие поставки обновляем всегда
PINNED_ORDER_NUMBERS = {"2000037545485", "2000037485754"}

# Cutoff-файл: всё, что создано раньше cutoff, не трогаем (кроме pinned)
FBO_CUTOFF_FILE = os.getenv("FBO_CUTOFF_FILE", "fbo_cutoff.json")

# Файл состояния для telegram/diff
FBO_SYNC_STATE_FILE = os.getenv("FBO_SYNC_STATE_FILE", "fbo_sync_state.json")

# Только “Подготовка к поставкам”
PREP_STATES = {"DATA_FILLING", "READY_TO_SUPPLY"}

# Триггеры для создания перемещения+отгрузки
SHIP_STATES = {"IN_TRANSIT", "ACCEPTANCE_AT_STORAGE_WAREHOUSE"}

# ==========================
# МОЙСКЛАД: ДАННЫЕ И ДИРЕКТ-HTTP (для move/demand)
# ==========================

MS_LOGIN = os.getenv("MS_LOGIN")
MS_PASSWORD = os.getenv("MS_PASSWORD")
MS_BASE_URL = "https://api.moysklad.ru/api/remap/1.2"

MS_ORGANIZATION_HREF = os.getenv("MS_ORGANIZATION_HREF")
MS_AGENT_HREF = os.getenv("MS_AGENT_HREF")

# Склад-источник (обычный склад) — нужен для перемещения
MS_STORE_HREF = os.getenv("MS_STORE_HREF")
# Склад FBO (назначение перемещения, и склад заказа)
MS_FBO_STORE_HREF = os.getenv("MS_FBO_STORE_HREF") or MS_STORE_HREF

# Статус заказа покупателя “FBO”
MS_STATE_FBO_HREF = os.getenv("MS_STATE_FBO_HREF") or os.getenv("MS_STATE_FBO")

# Опционально: статусы “Поставка” для перемещения/отгрузки
MS_STATE_SUPPLY_MOVE = os.getenv("MS_STATE_SUPPLY_MOVE")   # meta.href state для entity/move
MS_STATE_SUPPLY_DEMAND = os.getenv("MS_STATE_SUPPLY_DEMAND")  # meta.href state для entity/demand

if not MS_LOGIN or not MS_PASSWORD:
    raise RuntimeError("Не заданы MS_LOGIN / MS_PASSWORD в .env")

if not MS_ORGANIZATION_HREF or not MS_AGENT_HREF or not MS_FBO_STORE_HREF or not MS_STORE_HREF:
    raise RuntimeError(
        "Не заданы MS_ORGANIZATION_HREF / MS_AGENT_HREF / MS_STORE_HREF / MS_FBO_STORE_HREF. Проверь .env"
    )

_MS_AUTH = base64.b64encode(f"{MS_LOGIN}:{MS_PASSWORD}".encode("utf-8")).decode("utf-8")
_MS_HEADERS = {
    "Authorization": f"Basic {_MS_AUTH}",
    "Accept": "application/json;charset=utf-8",
    "Accept-Encoding": "gzip",
    "Content-Type": "application/json",
}

# ==========================
# ВСПОМОГАТЕЛЬНЫЕ
# ==========================

def _parse_ozon_dt(s: Optional[str]) -> Optional[datetime]:
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _to_ms_moment(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _ms_meta(href: str, type_: str) -> Dict[str, Any]:
    return {"href": href, "type": type_, "mediaType": "application/json"}


def _cluster_from_storage_name(storage_name: str) -> str:
    up = (storage_name or "").upper()
    if "ПУШКИНО" in up:
        return "Москва и МО"
    if "_" in (storage_name or ""):
        return storage_name.split("_", 1)[0]
    return storage_name or "—"


def _load_cutoff() -> Optional[datetime]:
    if not os.path.exists(FBO_CUTOFF_FILE):
        return None
    try:
        with open(FBO_CUTOFF_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return _parse_ozon_dt(data.get("cutoff"))
    except Exception:
        return None


def _save_cutoff(dt: datetime) -> None:
    dt = dt.astimezone(timezone.utc)
    s = dt.isoformat().replace("+00:00", "Z")
    with open(FBO_CUTOFF_FILE, "w", encoding="utf-8") as f:
        json.dump({"cutoff": s}, f, ensure_ascii=False, indent=2)


def _load_sync_state() -> Dict[str, Any]:
    if not os.path.exists(FBO_SYNC_STATE_FILE):
        return {"orders": {}}
    try:
        with open(FBO_SYNC_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f) or {"orders": {}}
    except Exception:
        return {"orders": {}}


def _save_sync_state(state: Dict[str, Any]) -> None:
    with open(FBO_SYNC_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _hash_positions(positions: List[Dict[str, Any]]) -> str:
    # стабильный хэш по (assortment.href, qty)
    items = []
    for p in positions:
        assort = p.get("assortment") or {}
        meta = assort.get("meta") or {}
        href = meta.get("href") or ""
        qty = int(p.get("quantity") or 0)
        items.append((href, qty))
    items.sort()
    raw = json.dumps(items, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _ms_get(url: str, params: Optional[dict] = None) -> dict:
    r = requests.get(url, headers=_MS_HEADERS, params=params, timeout=30)
    if r.status_code >= 400:
        print(f"[MS GET ERROR] {r.url} status={r.status_code} body={r.text[:500]}")
    r.raise_for_status()
    return r.json()


def _ms_post(url: str, payload: dict) -> dict:
    r = requests.post(url, headers=_MS_HEADERS, json=payload, timeout=30)
    if r.status_code >= 400:
        print(f"[MS POST ERROR] {r.url} status={r.status_code} body={r.text[:500]}")
    r.raise_for_status()
    return r.json()


def _ms_put(url: str, payload: dict) -> dict:
    r = requests.put(url, headers=_MS_HEADERS, json=payload, timeout=30)
    if r.status_code >= 400:
        print(f"[MS PUT ERROR] {r.url} status={r.status_code} body={r.text[:500]}")
    r.raise_for_status()
    return r.json()


def _ms_retry(fn, *args, **kwargs):
    # ретраи на 429 от МС
    max_tries = 6
    for attempt in range(1, max_tries + 1):
        try:
            return fn(*args, **kwargs)
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code == 429:
                wait = 0.8 * attempt
                print(f"[MS] 429 rate limit, попытка {attempt}/{max_tries}, ждём {wait:.1f}s")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("[MS] Не удалось выполнить запрос из-за постоянных 429")


def _ms_find_by_name(entity: str, name: str) -> Optional[dict]:
    url = f"{MS_BASE_URL}/entity/{entity}"
    params = {"filter": f"name={name}", "limit": 1}
    data = _ms_retry(_ms_get, url, params)
    rows = data.get("rows") or []
    return rows[0] if rows else None


def _ms_get_order_full(order_href: str) -> dict:
    return _ms_retry(_ms_get, order_href)


def _ms_get_order_positions(order_href: str) -> List[dict]:
    # позиции заказа лежат в /customerorder/<id>/positions
    url = f"{order_href}/positions"
    data = _ms_retry(_ms_get, url)
    rows = data.get("rows") or []
    return rows if isinstance(rows, list) else []


# ==========================
# ОЗОН → ПЛАНОВАЯ ДАТА + СКЛАД НАЗНАЧЕНИЯ
# ==========================

def _get_planned_dt(order: Dict[str, Any]) -> Optional[datetime]:
    supplies = order.get("supplies") or []
    if isinstance(supplies, list) and supplies:
        s0 = supplies[0] if isinstance(supplies[0], dict) else {}
        storage = s0.get("storage_warehouse") or {}
        if isinstance(storage, dict):
            arrival = storage.get("arrival_date")
            dt = _parse_ozon_dt(arrival)
            if dt:
                return dt
    return _parse_ozon_dt(order.get("created_date"))


def _get_storage_name(order: Dict[str, Any]) -> str:
    supplies = order.get("supplies") or []
    if isinstance(supplies, list) and supplies and isinstance(supplies[0], dict):
        storage = supplies[0].get("storage_warehouse") or {}
        if isinstance(storage, dict):
            return storage.get("name") or "—"
    return "—"


# ==========================
# СБОР ПОЗИЦИЙ ПО BUNDLE
# ==========================

def _collect_positions(order: Dict[str, Any], client: OzonFboClient) -> Tuple[List[Dict[str, Any]], List[str]]:
    positions: List[Dict[str, Any]] = []
    errors: List[str] = []

    supplies = order.get("supplies") or []
    if not isinstance(supplies, list):
        supplies = []

    for sup in supplies:
        if not isinstance(sup, dict):
            continue
        bundle_id = sup.get("bundle_id")
        if not bundle_id:
            continue

        items = client.get_bundle_items(bundle_id)
        print(f"[OZON FBO] Для bundle_id={bundle_id} ({client.account_name}) получено товаров: {len(items)}")

        for it in items:
            if not isinstance(it, dict):
                continue

            # ВАЖНО: берём артикул продавца, не SKU
            offer = it.get("offer_id") or it.get("vendor_code") or it.get("contractor_item_code")
            if not offer:
                # fallback: sku (если вдруг API не отдаст offer_id)
                sku = it.get("sku")
                if sku is not None:
                    offer = str(sku)

            if not offer:
                continue

            offer = str(offer).strip()

            qty = it.get("quantity") or 0
            try:
                qty = int(qty)
            except Exception:
                qty = 0
            if qty <= 0:
                continue

            product = _ms_retry(find_product_by_article, offer)
            if not product:
                errors.append(f"Товар с артикулом '{offer}' не найден в МойСклад")
                continue

            # цена из МС (salePrices[0].value)
            price = None
            sale_prices = product.get("salePrices")
            if isinstance(sale_prices, list) and sale_prices:
                first = sale_prices[0] or {}
                price = first.get("value")

            pos = {
                "quantity": qty,
                "assortment": {"meta": product["meta"]},
            }
            if price is not None:
                pos["price"] = price

            positions.append(pos)

    return positions, errors


# ==========================
# MOVE + DEMAND (1 на заявку)
# ==========================

def _ensure_move_and_demand(order_number: str, comment: str, ms_order: dict, ozon_state: str) -> None:
    """
    Создаёт перемещение (move) + отгрузку (demand), если их ещё нет.
    Проверяем существование по name == order_number.
    """
    # 1) Если уже есть demand — считаем, что всё сделано (1 отгрузка на 1 заявку)
    existing_demand = _ms_find_by_name("demand", order_number)
    if existing_demand:
        return

    # 2) Создаём/проверяем move
    existing_move = _ms_find_by_name("move", order_number)

    # Нужно достать позиции заказа
    order_href = (ms_order.get("meta") or {}).get("href")
    if not order_href:
        raise ValueError("У заказа нет meta.href, не могу создать перемещение/отгрузку")

    full_order = _ms_get_order_full(order_href)
    positions = _ms_get_order_positions(order_href)
    if not positions:
        # иногда позиции могут быть в самом заказе (редко)
        positions = full_order.get("positions", {}).get("rows") or []
    if not positions:
        raise ValueError("В заказе нет позиций для перемещения/отгрузки")

    move_payload = {
        "name": order_number,
        "organization": full_order.get("organization"),
        "sourceStore": {"meta": _ms_meta(MS_STORE_HREF, "store")},
        "targetStore": {"meta": _ms_meta(MS_FBO_STORE_HREF, "store")},
        "description": comment,
        "positions": [
            {
                "quantity": p.get("quantity", 0),
                "assortment": p.get("assortment"),
            }
            for p in positions
        ],
    }
    if MS_STATE_SUPPLY_MOVE:
        move_payload["state"] = {"meta": _ms_meta(MS_STATE_SUPPLY_MOVE, "state")}

    if not existing_move:
        print(f"[FBO] Создаём перемещение {order_number} (СКЛАД → FBO)")
        if not DRY_RUN_FBO:
            _ms_retry(_ms_post, f"{MS_BASE_URL}/entity/move", move_payload)
            send_telegram_message(f"✅ Создано перемещение по поставке №{order_number} (статус Ozon: {ozon_state})")
    else:
        # обновлять move не обязательно, но можем подравнять комментарий/позиции
        pass

    # 3) Создаём demand (отгрузку)
    demand_payload = {
        "name": order_number,
        "customerOrder": {"meta": full_order.get("meta")},
        "organization": full_order.get("organization"),
        "agent": full_order.get("agent"),
        # склад отгрузки логичнее = FBO (после перемещения)
        "store": {"meta": _ms_meta(MS_FBO_STORE_HREF, "store")},
        "description": comment,
        "positions": [
            {
                "quantity": p.get("quantity", 0),
                "assortment": p.get("assortment"),
                # цена берём из позиции заказа (если есть)
                "price": p.get("price", 0),
            }
            for p in positions
        ],
    }
    if MS_STATE_SUPPLY_DEMAND:
        demand_payload["state"] = {"meta": _ms_meta(MS_STATE_SUPPLY_DEMAND, "state")}

    print(f"[FBO] Создаём отгрузку {order_number} (1 на заявку)")
    if not DRY_RUN_FBO:
        _ms_retry(_ms_post, f"{MS_BASE_URL}/entity/demand", demand_payload)
        send_telegram_message(f"🚚 Создана отгрузка по поставке №{order_number} (статус Ozon: {ozon_state})")


# ==========================
# ОСНОВНАЯ ОБРАБОТКА 1 ПОСТАВКИ
# ==========================

def _process_one(order: Dict[str, Any], client: OzonFboClient, cutoff: Optional[datetime], sync_state: dict) -> None:
    order_number = str(order.get("order_number") or order.get("order_id") or "")
    oz_state = str(order.get("state") or "").upper()

    created_dt = _parse_ozon_dt(order.get("created_date"))
    updated_dt = _parse_ozon_dt(order.get("state_updated_date"))

    # Для отсечки используем более "живую" дату:
    # - если есть state_updated_date → берём её (она отражает реальные изменения)
    # - иначе fallback на created_date
    ref_dt = updated_dt or created_dt

    # pinned — всегда
    if order_number not in PINNED_ORDER_NUMBERS:
        # остальные — только если ref_dt >= cutoff
        if cutoff and ref_dt and ref_dt < cutoff:
            return

    # Берём склад назначения + кластер
    storage_name = _get_storage_name(order)
    cluster = _cluster_from_storage_name(storage_name)

    # Комментарий: НИКАКОГО “Красноярск” — только номер/кластер/склад назначения
    comment = f"{order_number} - {cluster} - {storage_name}"

    planned_dt = _get_planned_dt(order)
    planned_ms = _to_ms_moment(planned_dt)

    # Позиции
    positions_payload, pos_errors = _collect_positions(order, client)

    print(
        f"[FBO] Обработка заявки {order_number} "
        f"(аккаунт={client.account_name}, state={oz_state}), "
        f"позиций={len(positions_payload)}, DRY_RUN={DRY_RUN_FBO}"
    )

    if not positions_payload:
        if pos_errors:
            print(f"[FBO] {order_number}: нет позиций МС. Примеры: {pos_errors[:5]}")
        return

    # payload заказа в МС
    payload: Dict[str, Any] = {
        "name": order_number,
        "organization": {"meta": _ms_meta(MS_ORGANIZATION_HREF, "organization")},
        "agent": {"meta": _ms_meta(MS_AGENT_HREF, "counterparty")},
        "store": {"meta": _ms_meta(MS_FBO_STORE_HREF, "store")},
        "description": comment,
        "positions": positions_payload,
    }

    if planned_ms:
        payload["shipmentPlannedMoment"] = planned_ms
        payload["deliveryPlannedMoment"] = planned_ms

    if MS_STATE_FBO_HREF:
        payload["state"] = {"meta": _ms_meta(MS_STATE_FBO_HREF, "state")}

    # ===== create/update заказа в МС =====
    existing = None
    if not DRY_RUN_FBO:
        existing = _ms_retry(find_customer_order_by_name, order_number)

    created_or_updated = "none"
    if existing:
        if not DRY_RUN_FBO:
            href = existing["meta"]["href"]
            _ms_retry(update_customer_order, href, payload)
        created_or_updated = "updated"
    else:
        if not DRY_RUN_FBO:
            _ms_retry(create_customer_order, payload)
        created_or_updated = "created"

    # ===== Telegram уведомления: создана/изменена =====
    orders_state = sync_state.setdefault("orders", {})
    prev = orders_state.get(order_number, {})

    pos_hash = _hash_positions(positions_payload)
    changed_fields = []

    if prev.get("planned_ms") != planned_ms:
        changed_fields.append(f"дата={planned_ms or '—'}")
    if prev.get("pos_hash") != pos_hash:
        changed_fields.append("состав=изменён")

    # Сообщение про создание
    if created_or_updated == "created" and not DRY_RUN_FBO:
        send_telegram_message(f"🆕 Создана поставка №{order_number} на склад {storage_name}")
    # Сообщение про изменение (только если реально менялось)
    elif created_or_updated == "updated" and changed_fields and not DRY_RUN_FBO:
        send_telegram_message(
            f"✏️ Изменена поставка №{order_number} на склад {storage_name}: " + ", ".join(changed_fields)
        )

    # сохраняем snapshot
    orders_state[order_number] = {
        "planned_ms": planned_ms,
        "pos_hash": pos_hash,
        "oz_state": oz_state,
        "storage_name": storage_name,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    # ===== move + demand при переходе в нужные статусы =====
    if oz_state in SHIP_STATES:
        # достаём заказ из МС заново (чтобы иметь meta.href)
        ms_order = existing
        if not ms_order and not DRY_RUN_FBO:
            ms_order = _ms_retry(find_customer_order_by_name, order_number)

        if ms_order:
            # создаём только 1 раз (проверка по name внутри)
            _ensure_move_and_demand(order_number, comment, ms_order, oz_state)


# ==========================
# ОСНОВНОЙ ЗАПУСК
# ==========================

def sync_fbo_supplies(limit: int = 50, days_back: int = 30) -> None:
    print(
        f"Запуск синхронизации FBO-поставок "
        f"(limit={limit}, days_back={days_back}, DRY_RUN={DRY_RUN_FBO})"
    )

    clients: List[OzonFboClient] = []

    oz1_id = os.getenv("OZON_CLIENT_ID")
    oz1_key = os.getenv("OZON_API_KEY")
    if oz1_id and oz1_key:
        clients.append(OzonFboClient(oz1_id, oz1_key, account_name="ozon1"))

    oz2_id = os.getenv("OZON2_CLIENT_ID")
    oz2_key = os.getenv("OZON2_API_KEY")
    if oz2_id and oz2_key:
        clients.append(OzonFboClient(oz2_id, oz2_key, account_name="ozon2"))

    if not clients:
        print("[FBO] Нет настроенных кабинетов Ozon для FBO (проверь .env)")
        return

    cutoff = _load_cutoff()
    if cutoff is None and not DRY_RUN_FBO:
        cutoff = datetime.now(timezone.utc)
        _save_cutoff(cutoff)
        print(f"[FBO] Установлена отсечка для новых поставок: {cutoff.isoformat()}")
    else:
        print(f"[FBO] Текущая отсечка: {cutoff.isoformat() if cutoff else 'нет'}")

    sync_state = _load_sync_state()

    for client in clients:
        try:
            orders = client.get_supply_orders(limit=limit, days_back=days_back)
        except Exception as e:
            print(f"[FBO] Ошибка получения списка поставок ({client.account_name}): {e!r}")
            continue

        print(f"[FBO] Кабинет {client.account_name}: получено заявок: {len(orders)}")

        for order in orders:
            try:
                _process_one(order, client, cutoff=cutoff, sync_state=sync_state)
            except Exception as e:
                num = str(order.get("order_number") or order.get("order_id") or "")
                print(f"[FBO] Ошибка обработки заявки {num} ({client.account_name}): {e!r}")
                continue

    if not DRY_RUN_FBO:
        _save_sync_state(sync_state)


if __name__ == "__main__":
    sync_fbo_supplies(limit=50, days_back=30)
