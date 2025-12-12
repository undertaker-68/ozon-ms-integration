import os
import json
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

from ozon_fbo_client import OzonFboClient
from ms_client import (
    find_product_by_article,
    create_customer_order,
    find_customer_order_by_name,
    update_customer_order,
    create_demand_from_order,
    MS_BASE_URL,
    HEADERS as MS_HEADERS,
)
from notifier import send_telegram_message

load_dotenv()

# ==========================
# НАСТРОЙКИ ОКРУЖЕНИЯ
# ==========================

# Ozon 1
OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID")
OZON_API_KEY = os.getenv("OZON_API_KEY")

# Ozon 2 (Trail Gear)
OZON2_CLIENT_ID = os.getenv("OZON2_CLIENT_ID")
OZON2_API_KEY = os.getenv("OZON2_API_KEY")

DRY_RUN_FBO = os.getenv("DRY_RUN_FBO", "false").lower() == "true"

# МойСклад: базовые сущности
MS_ORGANIZATION_HREF = os.getenv("MS_ORGANIZATION_HREF")
MS_AGENT_HREF = os.getenv("MS_AGENT_HREF")
MS_STORE_HREF = os.getenv("MS_STORE_HREF")

# Отдельный склад/контрагент для FBO (если не заданы — используем обычные)
MS_FBO_STORE_HREF = os.getenv("MS_FBO_STORE_HREF") or MS_STORE_HREF
MS_FBO_AGENT_HREF = os.getenv("MS_FBO_AGENT_HREF") or MS_AGENT_HREF

# Статус заказа для FBO (опционально)
MS_STATE_FBO_HREF = os.getenv("MS_STATE_FBO_HREF") or os.getenv("MS_STATE_FBO")

# Статус отгрузки для FBO (опционально)
MS_FBO_DEMAND_STATE_HREF = os.getenv("MS_FBO_DEMAND_STATE_HREF")

# Статус перемещения "Поставка" (опционально)
MS_STATE_MOVE_FBO_HREF = os.getenv("MS_STATE_MOVE_FBO_HREF")

if not MS_ORGANIZATION_HREF or not MS_FBO_AGENT_HREF or not MS_FBO_STORE_HREF:
    raise RuntimeError("Не заданы MS_ORGANIZATION_HREF / MS_AGENT_HREF / MS_STORE_HREF в .env")

# Состояния поставок Ozon, при которых создаём ОТГРУЗКУ в МС
DEMAND_CREATE_SUPPLY_STATES = {
    "IN_TRANSIT",
    "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
}

# Файл состояния для FBO (чтобы не трогать старые поставки на самом первом запуске)
FBO_STATE_FILE = "fbo_state.json"


# ==========================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ: ДАТЫ
# ==========================

def _parse_ozon_datetime(dt_str: str | None) -> datetime | None:
    if not dt_str:
        return None
    dt_str = dt_str.strip()
    if not dt_str:
        return None

    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1] + "+00:00"

    try:
        return datetime.fromisoformat(dt_str)
    except Exception:  # noqa: BLE001
        return None


def _to_ms_moment(dt: datetime | None) -> str | None:
    if not dt:
        return None
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%d %H:%M:%S")


def _build_ms_meta(href: str, type_: str) -> dict:
    return {
        "href": href,
        "type": type_,
        "mediaType": "application/json",
    }


# ==========================
# СОСТОЯНИЕ FBO
# ==========================

def _load_fbo_state() -> dict | None:
    if not os.path.exists(FBO_STATE_FILE):
        return None

    try:
        with open(FBO_STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception as e:  # noqa: BLE001
        print(f"[FBO] Не удалось прочитать {FBO_STATE_FILE}: {e!r}")
        return None

    if not isinstance(state, dict):
        return None

    state.setdefault("version", 1)
    state.setdefault("accounts", {})
    if not isinstance(state["accounts"], dict):
        state["accounts"] = {}

    for acc in ("ozon1", "ozon2"):
        ids = state["accounts"].get(acc)
        if not isinstance(ids, list):
            state["accounts"][acc] = []
        else:
            state["accounts"][acc] = [
                int(x) for x in ids
                if isinstance(x, int) or (isinstance(x, str) and x.isdigit())
            ]

    return state


def _save_fbo_state(state: dict) -> None:
    try:
        with open(FBO_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:  # noqa: BLE001
        print(f"[FBO] Не удалось записать {FBO_STATE_FILE}: {e!r}")


# ==========================
# МОЙСКЛАД: ОТГРУЗКИ / ПЕРЕМЕЩЕНИЯ
# ==========================

def _has_demand_for_order(order_href: str) -> bool:
    """
    Проверяем, есть ли у заказа хотя бы одна отгрузка.
    Используем подресурс /customerorder/{id}/demands
    вместо фильтра по /entity/demand.
    """
    if not order_href:
        return False

    url = order_href.rstrip("/") + "/demands"
    params = {"limit": 1}

    try:
        r = requests.get(url, headers=MS_HEADERS, params=params, timeout=30)
    except Exception as e:  # noqa: BLE001
        print(f"[MS] Ошибка запроса списка отгрузок по заказу: {e!r}")
        return False

    if r.status_code in (404, 410):
        return False

    if r.status_code >= 400:
        print(f"[MS] Ошибка получения отгрузок: {r.status_code} {r.text[:500]}")
        return False

    try:
        data = r.json()
    except Exception as e:  # noqa: BLE001
        print(f"[MS] Ошибка парсинга ответа при получении отгрузок: {e!r}")
        return False

    rows = data.get("rows") or []
    return bool(rows)


def _has_move_for_order(order_href: str) -> bool:
    """
    Проверяем, есть ли у заказа хотя бы одно перемещение.
    /customerorder/{id}/moves
    """
    if not order_href:
        return False

    url = order_href.rstrip("/") + "/moves"
    params = {"limit": 1}

    try:
        r = requests.get(url, headers=MS_HEADERS, params=params, timeout=30)
    except Exception as e:  # noqa: BLE001
        print(f"[MS] Ошибка запроса списка перемещений по заказу: {e!r}")
        return False

    if r.status_code in (404, 410):
        return False

    if r.status_code >= 400:
        print(f"[MS] Ошибка получения перемещений: {r.status_code} {r.text[:500]}")
        return False

    try:
        data = r.json()
    except Exception as e:  # noqa: BLE001
        print(f"[MS] Ошибка парсинга ответа при получении перемещений: {e!r}")
        return False

    rows = data.get("rows") or []
    return bool(rows)


def _update_demand_state(demand: dict) -> None:
    if not MS_FBO_DEMAND_STATE_HREF:
        return

    meta = (demand or {}).get("meta") or {}
    href = meta.get("href")
    if not href:
        return

    payload = {
        "state": {
            "meta": _build_ms_meta(MS_FBO_DEMAND_STATE_HREF, "state"),
        }
    }

    try:
        r = requests.put(href, headers=MS_HEADERS, json=payload, timeout=30)
    except Exception as e:  # noqa: BLE001
        print(f"[MS] Ошибка запроса при обновлении статуса отгрузки: {e!r}")
        return

    if r.status_code >= 400:
        print(f"[MS] Ошибка обновления статуса отгрузки: {r.status_code} {r.text[:500]}")
        return

    print("[MS] Статус отгрузки FBO обновлён.")


def _create_move_from_order(order: dict) -> dict:
    """
    Создаёт перемещение (move) со склада СКЛАД на склад FBO
    на основании позиций заказа.
    """
    order_meta = order.get("meta") or {}
    order_href = order_meta.get("href")
    if not order_href:
        raise ValueError("У заказа нет meta.href, не можем создать перемещение")

    # На всякий случай добираем полный заказ, если нет позиций
    if not order.get("positions"):
        url = order_href
        r = requests.get(url, headers=MS_HEADERS, timeout=30)
        r.raise_for_status()
        order = r.json()

    positions = order.get("positions") or []
    if not isinstance(positions, list):
        positions = []

    move_positions: list[dict] = []
    for pos in positions:
        if not isinstance(pos, dict):
            continue
        assort = pos.get("assortment")
        if not assort:
            continue
        move_positions.append(
            {
                "quantity": pos.get("quantity", 0) or 0,
                "assortment": assort,
            }
        )

    if not move_positions:
        raise ValueError("В заказе нет позиций для перемещения")

    # Организация/склады
    org_meta = order.get("organization", {}).get("meta") or _build_ms_meta(
        MS_ORGANIZATION_HREF, "organization"
    )

    source_store_meta = order.get("store", {}).get("meta") or _build_ms_meta(
        MS_STORE_HREF, "store"
    )
    target_store_meta = _build_ms_meta(MS_FBO_STORE_HREF, "store")

    payload: dict = {
        "organization": {"meta": org_meta},
        "sourceStore": {"meta": source_store_meta},
        "targetStore": {"meta": target_store_meta},
        "positions": move_positions,
        # Комментарий один в один из заказа
        "description": order.get("description"),
    }

    # Статус перемещения "Поставка", если задан
    if MS_STATE_MOVE_FBO_HREF:
        payload["state"] = {
            "meta": _build_ms_meta(MS_STATE_MOVE_FBO_HREF, "state"),
        }

    move_name = order.get("name")
    if move_name:
        payload["name"] = move_name

    url = f"{MS_BASE_URL}/entity/move"
    r = requests.post(url, headers=MS_HEADERS, json=payload, timeout=30)
    if r.status_code >= 400:
        print(f"[MS] Ошибка создания перемещения: {r.status_code} {r.text[:500]}")
        r.raise_for_status()
    move = r.json()
    print(f"[MS] Создано перемещение по заказу {move_name}")
    return move


# ==========================
# СОСТАВ ПОСТАВКИ
# ==========================

def _collect_positions_from_supply(order: dict, client: OzonFboClient) -> tuple[list[dict], list[str]]:
    """
    Собираем позиции поставки из всех bundle в заявке FBO.
    Возвращаем:
      - список позиций (ms_meta + quantity + price)
      - список текстов ошибок (по товарам, которые не удалось сопоставить)
    """
    all_positions: list[dict] = []
    errors: list[str] = []

    supplies = order.get("supplies") or []
    if not isinstance(supplies, list):
        supplies = []

    order_id = order.get("order_id")

    for supply in supplies:
        if not isinstance(supply, dict):
            continue

        bundle_id = supply.get("bundle_id")
        if not bundle_id:
            continue

        # Получаем товары по bundle_id
        items = client.get_bundle_items(bundle_id)
        print(
            f"[OZON FBO] Для bundle_id={bundle_id} ({client.account_name}) "
            f"получено товаров: {len(items)}"
        )

        for item in items:
            if not isinstance(item, dict):
                continue

            # --- ВАЖНО: берём именно артикул продавца, а не sku ---
            offer = (
                item.get("offer_id")
                or item.get("vendor_code")
                or item.get("contractor_item_code")
            )

            if offer is None or str(offer).strip() == "":
                # В крайнем случае пробуем sku
                sku = item.get("sku")
                if sku is not None:
                    offer = str(sku)

            if not offer:
                msg = (
                    f"[FBO] В позиции bundle_id={bundle_id} заявки {order_id} "
                    f"нет offer_id / vendor_code / contractor_item_code / sku"
                )
                print(msg)
                errors.append(msg)
                continue

            offer = str(offer).strip()
            quantity = item.get("quantity") or 0
            if quantity <= 0:
                continue

            product = find_product_by_article(offer)
            if not product:
                msg = (
                    f"[FBO] Товар с артикулом '{offer}' не найден в МойСклад "
                    f"(bundle_id={bundle_id}, заявка {order_id})"
                )
                print(msg)
                errors.append(msg)
                continue

            # Базовая цена продажи из МойСклад (salePrices[0].value, в копейках)
            price = None
            sale_prices = product.get("salePrices")
            if isinstance(sale_prices, list) and sale_prices:
                first_price = sale_prices[0] or {}
                price = first_price.get("value")

            all_positions.append(
                {
                    "ms_meta": product["meta"],
                    "quantity": quantity,
                    "price": price,
                }
            )

    return all_positions, errors


# ==========================
# ОБРАБОТКА ОДНОЙ ЗАЯВКИ
# ==========================

def _process_single_fbo_order(order: dict, client: OzonFboClient, dry_run: bool) -> None:
    """
    Обработка одной FBO-заявки:
      - создаём/обновляем заказ в МойСклад
      - проставляем статус FBO (если MS_STATE_FBO_HREF)
      - при нужных состояниях создаём перемещение и отгрузку
    """
    ozon_account = order.get("_ozon_account") or "ozon1"
    order_id = order.get("_order_id") or order.get("order_id")

    order_number = (
        order.get("order_number")
        or (str(order_id) if order_id is not None else "")
    )

    if not order_number:
        print("[FBO] Пропускаем заявку без номера.")
        return

    supplies = order.get("supplies") or []
    if not isinstance(supplies, list):
        supplies = []

    # Состояния по поставке (supplies[].state)
    supply_states = {
        str((s or {}).get("state") or "").upper()
        for s in supplies
        if isinstance(s, dict)
    }
    order_state = (order.get("state") or "").upper()

    # Если сама заявка уже CANCELLED — просто пропускаем, ничего не создаём
    if order_state == "CANCELLED":
        print(f"[FBO] Заявка {order_number} state=CANCELLED — пропускаем.")
        return

    # Склад назначения (storage_warehouse) первой supply
    first_supply = supplies[0] if supplies else {}
    storage_wh = (first_supply.get("storage_warehouse") or {}) if first_supply else {}
    storage_name = storage_wh.get("name") or "N/A"

    # Попробуем кластер взять из drop_off_warehouse.name — больше не используем,
    # комментарий формируем только как <номер заявки> - <склад назначения>.
    dropoff_wh = order.get("drop_off_warehouse") or {}
    cluster_name = ""

    # Плановая дата: arrival_date склада назначения / timeslot / created_date
    arrival_raw = storage_wh.get("arrival_date") or first_supply.get("arrival_date")
    if not arrival_raw:
        timeslot_block = (order.get("timeslot") or {}).get("timeslot") or {}
        arrival_raw = timeslot_block.get("from") or order.get("created_date")

    arrival_dt = _parse_ozon_datetime(arrival_raw) if isinstance(arrival_raw, str) else None
    planned_moment = _to_ms_moment(arrival_dt)

    # Комментарий: номер заявки - склад назначения
    comment_parts = [order_number]
    if storage_name:
        comment_parts.append(storage_name)
    comment = " - ".join(comment_parts)

    raw_positions, pos_errors = _collect_positions_from_supply(order, client)

    if not raw_positions and pos_errors:
        msg = f"По FBO-поставке {order_number} не удалось подобрать ни одной позиции МойСклад."
        print("[FBO] " + msg)
        text = f"❗ {msg}\n" + "\n".join(pos_errors[:10])
        try:
            send_telegram_message(text)
        except Exception:
            pass
        return

    positions_payload: list[dict] = []
    for pos in raw_positions:
        item_payload = {
            "quantity": pos["quantity"],
            "assortment": {"meta": pos["ms_meta"]},
        }
        if pos.get("price") is not None:
            item_payload["price"] = pos["price"]
        positions_payload.append(item_payload)

    org_meta = _build_ms_meta(MS_ORGANIZATION_HREF, "organization")
    agent_meta = _build_ms_meta(MS_FBO_AGENT_HREF, "counterparty")
    store_meta = _build_ms_meta(MS_FBO_STORE_HREF, "store")

    total_qty = sum(p.get("quantity", 0) for p in positions_payload)

    print(
        f"[FBO] Обработка заявки {order_number} "
        f"(аккаунт={ozon_account}, state={order_state}, "
        f"supply_states={','.join(sorted(supply_states))}), "
        f"позиций: {len(positions_payload)}, всего штук: {total_qty}, DRY_RUN={dry_run}"
    )

    order_name = order_number

    payload: dict = {
        "name": order_name,
        "organization": {"meta": org_meta},
        "agent": {"meta": agent_meta},
        "store": {"meta": store_meta},
        "positions": positions_payload,
        "description": comment,
    }

    # Плановая дата в оба поля, чтобы точно подсветилась в UI
    if planned_moment:
        payload["shipmentPlannedMoment"] = planned_moment
        payload["deliveryPlannedMoment"] = planned_moment

    # Статус заказа "FBO" (если задан)
    if MS_STATE_FBO_HREF:
        payload["state"] = {
            "meta": _build_ms_meta(MS_STATE_FBO_HREF, "state"),
        }

    if dry_run:
        print(f"[FBO] DRY_RUN: заказ {order_name} не создаём/не обновляем.")
        return
try:
    existing = find_customer_order_by_name(order_name)
except requests.exceptions.HTTPError as e:
    if "429" in str(e):
        print(f"[MS] Rate limit при поиске заказа {order_name}, пропускаем")
        time.sleep(1.2)
        return
    raise
    
    if existing:
        order_href = existing["meta"]["href"]

        old_planned = existing.get("shipmentPlannedMoment") or existing.get("deliveryPlannedMoment")
        old_desc = existing.get("description")
        old_positions_meta = (existing.get("positions") or {}).get("meta") or {}
        old_positions_count = old_positions_meta.get("size")

        update_payload: dict = {
            "positions": positions_payload,
            "description": comment,
        }
        if planned_moment:
            update_payload["shipmentPlannedMoment"] = planned_moment
            update_payload["deliveryPlannedMoment"] = planned_moment
        if MS_STATE_FBO_HREF:
            update_payload["state"] = {
                "meta": _build_ms_meta(MS_STATE_FBO_HREF, "state"),
            }

        update_customer_order(order_href, update_payload)

        changes: list[str] = []

        if planned_moment and planned_moment != old_planned:
            changes.append(f"плановая дата: {old_planned or '—'} → {planned_moment}")

        if comment != old_desc:
            changes.append("комментарий обновлён")

        if old_positions_count is not None and old_positions_count != len(positions_payload):
            changes.append(f"кол-во позиций: {old_positions_count} → {len(positions_payload)}")

        if old_positions_count is None:
            changes.append(f"кол-во позиций: {len(positions_payload)}")

        if changes:
            text = (
                f"Изменена поставка №{order_number} "
                f"на склад {storage_name} (аккаунт {ozon_account})\n"
                + "; ".join(changes)
            )
            try:
                send_telegram_message(text)
            except Exception:
                pass

        ms_order = existing
        ms_order_href = order_href
    else:
        ms_order = create_customer_order(payload)
        ms_order_href = ms_order["meta"]["href"]

        text = (
            f"Создана поставка №{order_number} "
            f"на склад {storage_name} (аккаунт {ozon_account})"
        )
        try:
            send_telegram_message(text)
        except Exception:
            pass

    # === ПЕРЕМЕЩЕНИЕ + ОТГРУЗКА ===

    need_demand = bool(supply_states & DEMAND_CREATE_SUPPLY_STATES)

    if need_demand:
        # 1) Перемещение со склада СКЛАД на FBO (если ещё нет)
        if not _has_move_for_order(ms_order_href):
            print(f"[FBO] Для заказа {order_name} создаём перемещение (СКЛАД → FBO).")
            _create_move_from_order(ms_order)

        # 2) Отгрузка по заказу (если ещё нет)
        if not _has_demand_for_order(ms_order_href):
            print(
                f"[FBO] Для заказа {order_name} (поставка {order_number}) "
                f"создаём отгрузку (состояния поставки: {supply_states})"
            )
            demand = create_demand_from_order(ms_order)
            _update_demand_state(demand)

            text = (
                f"По поставке №{order_number} создана отгрузка в МойСклад "
                f"(аккаунт {ozon_account}, склад {storage_name})"
            )
            try:
                send_telegram_message(text)
            except Exception:
                pass
        else:
            print(
                f"[FBO] Для заказа {order_name} отгрузка уже существует, "
                f"повторно не создаём."
            )
    else:
        print(
            f"[FBO] Для заказа {order_name} отгрузка не создаётся: "
            f"supply_states={supply_states}, need_demand={need_demand}"
        )


# ==========================
# ОСНОВНАЯ ФУНКЦИЯ
# ==========================

def sync_fbo_supplies(limit: int = 50, days_back: int = 30, dry_run: bool | None = None) -> None:
    """
    Основная функция синхронизации FBO-поставок в МойСклад.

    ВАЖНО:
      - первый запуск (когда нет fbo_state.json) просто
        запоминает все текущие заявки и НИЧЕГО не создаёт в МС;
      - последующие запуски обрабатывают все заявки в окне days_back,
        чтобы актуальные поставки обновлялись по дате и составу.
    """
    if dry_run is None:
        dry_run = DRY_RUN_FBO

    print(
        f"Запуск синхронизации FBO-поставок "
        f"(limit={limit}, days_back={days_back}, DRY_RUN={dry_run})"
    )

    clients: list[OzonFboClient] = []

    if OZON_CLIENT_ID and OZON_API_KEY:
        clients.append(OzonFboClient(OZON_CLIENT_ID, OZON_API_KEY, account_name="ozon1"))
    else:
        print("[FBO] Не заданы OZON_CLIENT_ID / OZON_API_KEY — первый кабинет пропущен.")

    if OZON2_CLIENT_ID and OZON2_API_KEY:
        clients.append(OzonFboClient(OZON2_CLIENT_ID, OZON2_API_KEY, account_name="ozon2"))
    else:
        print("[FBO] Не заданы OZON2_CLIENT_ID / OZON2_API_KEY — второй кабинет пропущен.")

    if not clients:
        print("[FBO] Нет доступных кабинетов Ozon для синхронизации.")
        return

    state = _load_fbo_state()
    first_run = state is None

    if state is None:
        state = {"version": 1, "accounts": {}}
    accounts_state: dict = state.setdefault("accounts", {})

    for client in clients:
        acc = client.account_name
        known_ids: list[int] = accounts_state.get(acc) or []
        accounts_state[acc] = known_ids

        try:
            orders = client.get_supply_orders(limit=limit, days_back=days_back)
        except Exception as e:  # noqa: BLE001
            print(f"[FBO] Ошибка получения заявок по кабинету {acc}: {e!r}")
            continue

        print(
            f"[FBO] Кабинет {acc}: получено заявок на поставку: "
            f"{len(orders)}"
        )

        # Первый запуск: только запоминаем ID, ничего не создаём
        if first_run and not dry_run:
            for order in orders:
                oid = order.get("_order_id") or order.get("order_id")
                if isinstance(oid, int) and oid not in known_ids:
                    known_ids.append(oid)
            print(
                f"[FBO] Первый запуск: для кабинета {acc} добавлено "
                f"{len(orders)} заявок в список известных, без создания документов."
            )
            continue

        # Обычный режим: всегда обрабатываем заявки (в пределах days_back),
        # чтобы актуальные поставки продолжали обновляться по дате и составу.
        for order in orders:
            oid = order.get("_order_id") or order.get("order_id")
            if not isinstance(oid, int):
                continue

            _process_single_fbo_order(order, client, dry_run=dry_run)

    if not dry_run:
        _save_fbo_state(state)


if __name__ == "__main__":
    sync_fbo_supplies(limit=50, days_back=30, dry_run=DRY_RUN_FBO)
