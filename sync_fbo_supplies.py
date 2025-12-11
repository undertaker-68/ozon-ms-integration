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
MS_STATE_FBO_HREF = os.getenv("MS_STATE_FBO_HREF")

# Статус отгрузки для FBO (опционально)
MS_FBO_DEMAND_STATE_HREF = os.getenv("MS_FBO_DEMAND_STATE_HREF")

if not MS_ORGANIZATION_HREF or not MS_FBO_AGENT_HREF or not MS_FBO_STORE_HREF:
    raise RuntimeError("Не заданы MS_ORGANIZATION_HREF / MS_AGENT_HREF / MS_STORE_HREF в .env")

# Состояния поставок Ozon, при которых создаём Отгрузку в МС
DEMAND_CREATE_SUPPLY_STATES = {
    "IN_TRANSIT",
    "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
}

# Файл состояния для FBO (чтобы не трогать старые поставки)
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
# МОЙСКЛАД: ОТГРУЗКИ
# ==========================

def _has_demand_for_order(order_href: str) -> bool:
    url = f"{MS_BASE_URL}/entity/demand"
    params = {
        "filter": f"customerOrder={order_href}",
        "limit": 1,
    }

    try:
        r = requests.get(url, headers=MS_HEADERS, params=params, timeout=30)
    except Exception as e:  # noqa: BLE001
        print(f"[MS] Ошибка запроса списка отгрузок по заказу: {e!r}")
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


# ==========================
# СОСТАВ ПОСТАВКИ
# ==========================

def _collect_positions_from_supply(order: dict, client: OzonFboClient) -> tuple[list[dict], list[str]]:
    """
    Собираем позиции поставки из всех bundle в заявке FBO.
    Возвращаем:
      - список позиций (ms_meta + quantity)
      - список текстов ошибок (по товарам, которые не удалось сопоставить)
    """
    all_positions: list[dict] = []
    errors: list[str] = []

    supplies = order.get("supplies") or []
    order_id = order.get("order_id")

    for supply in supplies:
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
            # --- ВАЖНО: берём именно артикул продавца, а не sku ---
            offer = (
                item.get("offer_id")
                or item.get("vendor_code")
                or item.get("contractor_item_code")
            )

            if offer is None or str(offer).strip() == "":
                # В самом крайнем случае пробуем sku (если вдруг в МС артикулы = sku)
                sku = item.get("sku")
                if sku is not None:
                    offer = str(sku)

            if not offer:
                msg = (
                    f"[FBO] В позиции bundle_id={bundle_id} заявки {order_id} "
                    f"нет offer_id / vendor_code / contractor_item_code / sku"
                )
                print(msg)
                # В Telegram по этой мелочи не шлём, чтобы не спамить
                errors.append(msg)
                continue

            offer = str(offer).strip()
            quantity = item.get("quantity") or 0

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

def _process_single_fbo_order(order: dict, client: OzonFboClient, dry_run: bool) -> None:
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

    first_supply = supplies[0] if supplies else {}

    supply_states = {
        str((s or {}).get("state") or "").upper()
        for s in supplies
    }
    order_state = (order.get("state") or "").upper()

    storage_wh = (first_supply.get("storage_warehouse") or {}) if first_supply else {}
    storage_name = storage_wh.get("name") or "N/A"

    dropoff_wh = order.get("drop_off_warehouse") or {}
    cluster_name = dropoff_wh.get("name") or ""

    arrival_raw = storage_wh.get("arrival_date") or first_supply.get("arrival_date")
    if not arrival_raw:
        timeslot = (first_supply.get("timeslot") or {}).get("timeslot") or {}
        arrival_raw = timeslot.get("from") or order.get("created_date")

    arrival_dt = _parse_ozon_datetime(arrival_raw) if isinstance(arrival_raw, str) else None
    planned_moment = _to_ms_moment(arrival_dt)

    comment_parts = [order_number]
    if cluster_name:
        comment_parts.append(cluster_name)
    if storage_name:
        comment_parts.append(storage_name)
    comment = " - ".join(comment_parts)

    raw_positions, pos_errors = _collect_positions_from_supply(order, client)

    if not raw_positions and pos_errors:
        msg = f"По FBO-поставке {order_number} не удалось подобрать ни одной позиции МС."
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

    if planned_moment:
        payload["shipmentPlannedMoment"] = planned_moment

    if MS_STATE_FBO_HREF:
        payload["state"] = {
            "meta": _build_ms_meta(MS_STATE_FBO_HREF, "state"),
        }

    if dry_run:
        print(f"[FBO] DRY_RUN: заказ {order_name} не создаём/не обновляем.")
        return

    existing = find_customer_order_by_name(order_name)

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

    if (
        supply_states & DEMAND_CREATE_SUPPLY_STATES
        and not _has_demand_for_order(ms_order_href)
    ):
        print(
            f"[FBO] Для заказа {order_name} (поставка {order_number}) "
            f"создаём отгрузку (состояния: {supply_states})"
        )
        if not dry_run:
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
            f"[FBO] Для заказа {order_name} отгрузка не создаётся: "
            f"supply_states={supply_states}, "
            f"есть_отгрузка={_has_demand_for_order(ms_order_href)}"
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
      - последующие запуски обрабатывают только новые order_id.
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

        for order in orders:
            oid = order.get("_order_id") or order.get("order_id")
            if not isinstance(oid, int):
                continue

            if oid in known_ids:
                continue

            _process_single_fbo_order(order, client, dry_run=dry_run)

            if not dry_run:
                known_ids.append(oid)

    if not dry_run:
        _save_fbo_state(state)


if __name__ == "__main__":
    sync_fbo_supplies(limit=50, days_back=30, dry_run=DRY_RUN_FBO)
