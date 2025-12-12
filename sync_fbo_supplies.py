import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

from ozon_fbo_client import OzonFboClient
from ms_client import (
    find_product_by_article,
    create_customer_order,
    find_customer_order_by_name,
    update_customer_order,
)

load_dotenv()

# ==========================
# НАСТРОЙКИ
# ==========================

DRY_RUN_FBO = os.getenv("DRY_RUN_FBO", "false").lower() == "true"

# ВРЕМЕННО: обновляем только эти 2 поставки, чтобы не трогать старые
ONLY_ORDER_NUMBERS = {"2000037545485", "2000037485754"}

# "Подготовка к поставкам" (по твоему выводу debug_fbo_list.py)
PREP_STATES = {"DATA_FILLING", "READY_TO_SUPPLY"}

# МойСклад: сущности (берём из .env как у тебя уже настроено)
MS_ORGANIZATION_HREF = os.getenv("MS_ORGANIZATION_HREF")
MS_AGENT_HREF = os.getenv("MS_AGENT_HREF")
MS_STORE_HREF = os.getenv("MS_STORE_HREF")

# FBO-склад и FBO-статус (ты добавил)
MS_FBO_STORE_HREF = os.getenv("MS_FBO_STORE_HREF") or MS_STORE_HREF
MS_STATE_FBO_HREF = os.getenv("MS_STATE_FBO_HREF") or os.getenv("MS_STATE_FBO")

if not MS_ORGANIZATION_HREF or not MS_AGENT_HREF or not MS_FBO_STORE_HREF:
    raise RuntimeError("Не заданы MS_ORGANIZATION_HREF / MS_AGENT_HREF / MS_FBO_STORE_HREF (или MS_STORE_HREF)")

# ==========================
# ВСПОМОГАТЕЛЬНЫЕ
# ==========================

def _parse_ozon_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
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
    up = storage_name.upper()
    if "ПУШКИНО" in up:
        return "Москва и МО"
    # простая эвристика: до первого "_" обычно город/кластер
    return storage_name.split("_", 1)[0] if "_" in storage_name else storage_name


def _get_planned_dt_from_order(order: Dict[str, Any]) -> Optional[datetime]:
    """
    Для плановой даты:
    1) storage_warehouse.arrival_date
    2) timeslot.from
    3) created_date
    """
    supplies = order.get("supplies") or []
    if isinstance(supplies, list) and supplies:
        first = supplies[0] or {}
        storage = (first.get("storage_warehouse") or {}) if isinstance(first, dict) else {}
        arrival = storage.get("arrival_date")
        dt = _parse_ozon_dt(arrival) if isinstance(arrival, str) else None
        if dt:
            return dt

    timeslot = (order.get("timeslot") or {}).get("timeslot") or {}
    if isinstance(timeslot, dict):
        dt = _parse_ozon_dt(timeslot.get("from"))
        if dt:
            return dt

    return _parse_ozon_dt(order.get("created_date"))


def _collect_positions(order: Dict[str, Any], client: OzonFboClient) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Берём товары по bundle и сопоставляем по артикулу продавца (offer_id/vendor_code/contractor_item_code).
    Цену ставим из МойСклад salePrices[0].value (копейки).
    """
    positions_payload: List[Dict[str, Any]] = []
    errors: List[str] = []

    supplies = order.get("supplies") or []
    if not isinstance(supplies, list):
        supplies = []

    for supply in supplies:
        if not isinstance(supply, dict):
            continue
        bundle_id = supply.get("bundle_id")
        if not bundle_id:
            continue

        items = client.get_bundle_items(bundle_id)
        print(f"[OZON FBO] Для bundle_id={bundle_id} ({client.account_name}) получено товаров: {len(items)}")

        for item in items:
            if not isinstance(item, dict):
                continue

            offer = (
                item.get("offer_id")
                or item.get("vendor_code")
                or item.get("contractor_item_code")
            )

            if offer is None or str(offer).strip() == "":
                # В крайнем случае — sku, но это хуже
                sku = item.get("sku")
                if sku is not None:
                    offer = str(sku)

            if not offer:
                continue

            offer = str(offer).strip()
            qty = item.get("quantity") or 0
            try:
                qty = int(qty)
            except Exception:
                qty = 0

            if qty <= 0:
                continue

            product = find_product_by_article(offer)
            if not product:
                errors.append(f"Товар с артикулом '{offer}' не найден в МойСклад")
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

            positions_payload.append(pos)

    return positions_payload, errors


# ==========================
# ОБРАБОТКА ОДНОЙ ПОСТАВКИ
# ==========================

def _process_single(order: Dict[str, Any], client: OzonFboClient, dry_run: bool) -> None:
    order_number = str(order.get("order_number") or order.get("order_id") or "")
    state = str(order.get("state") or "").upper()

    if ONLY_ORDER_NUMBERS and order_number not in ONLY_ORDER_NUMBERS:
        return

    if state not in PREP_STATES:
        print(f"[FBO] Пропуск {order_number}: state={state} (не подготовка)")
        return

    planned_dt = _get_planned_dt_from_order(order)
    planned_moment = _to_ms_moment(planned_dt)

    supplies = order.get("supplies") or []
    storage_name = "N/A"
    if isinstance(supplies, list) and supplies and isinstance(supplies[0], dict):
        storage_wh = (supplies[0].get("storage_warehouse") or {})
        if isinstance(storage_wh, dict):
            storage_name = storage_wh.get("name") or "N/A"

    cluster = _cluster_from_storage_name(storage_name)
    # Важно: убираем склад отгрузки (Красноярск и т.п.) полностью
    comment = f"{order_number} - {cluster} - {storage_name}"

    positions_payload, pos_errors = _collect_positions(order, client)

    print(
        f"[FBO] Обработка заявки {order_number} "
        f"(аккаунт={client.account_name}, state={state}), "
        f"позиций={len(positions_payload)}, DRY_RUN={dry_run}"
    )

    if not positions_payload:
        print(f"[FBO] {order_number}: нет позиций для МС. Ошибки: {pos_errors[:5]}")
        return

    payload: Dict[str, Any] = {
        "name": order_number,
        "organization": {"meta": _ms_meta(MS_ORGANIZATION_HREF, "organization")},
        "agent": {"meta": _ms_meta(MS_AGENT_HREF, "counterparty")},
        "store": {"meta": _ms_meta(MS_FBO_STORE_HREF, "store")},
        "description": comment,
        "positions": positions_payload,
    }

    if planned_moment:
        payload["shipmentPlannedMoment"] = planned_moment
        payload["deliveryPlannedMoment"] = planned_moment

    if MS_STATE_FBO_HREF:
        payload["state"] = {"meta": _ms_meta(MS_STATE_FBO_HREF, "state")}

    if dry_run:
        return

    # чуть снижаем RPS к МС (чтобы не ловить 429)
    time.sleep(0.25)

    existing = find_customer_order_by_name(order_number)

    if existing:
        href = existing["meta"]["href"]
        time.sleep(0.25)
        update_customer_order(href, payload)
        print(f"[FBO] Заказ {order_number} обновлён в МС")
    else:
        time.sleep(0.25)
        create_customer_order(payload)
        print(f"[FBO] Заказ {order_number} создан в МС")


# ==========================
# ОСНОВНОЙ ЗАПУСК
# ==========================

def sync_fbo_supplies(limit: int = 50, days_back: int = 30, dry_run: bool = False) -> None:
    print(
        f"Запуск синхронизации FBO-поставок "
        f"(limit={limit}, days_back={days_back}, DRY_RUN={dry_run})"
    )

    clients: List[OzonFboClient] = []

    # ozon1 — твои реальные переменные окружения
    oz1_id = os.getenv("OZON_CLIENT_ID")
    oz1_key = os.getenv("OZON_API_KEY")
    if oz1_id and oz1_key:
        clients.append(OzonFboClient(oz1_id, oz1_key, account_name="ozon1"))

    # ozon2 — твои реальные переменные окружения
    oz2_id = os.getenv("OZON2_CLIENT_ID")
    oz2_key = os.getenv("OZON2_API_KEY")
    if oz2_id and oz2_key:
        clients.append(OzonFboClient(oz2_id, oz2_key, account_name="ozon2"))

    if not clients:
        print("[FBO] Нет настроенных кабинетов Ozon для FBO (проверь .env)")
        return

    for client in clients:
        try:
            orders = client.get_supply_orders(limit=limit, days_back=days_back)
        except Exception as e:
            print(f"[FBO] Ошибка получения списка поставок ({client.account_name}): {e!r}")
            continue

        print(f"[FBO] Кабинет {client.account_name}: получено заявок: {len(orders)}")

        for order in orders:
            try:
                _process_single(order, client, dry_run=dry_run)
            except Exception as e:
                num = str(order.get("order_number") or order.get("order_id") or "")
                print(f"[FBO] Ошибка обработки заявки {num} ({client.account_name}): {e!r}")
                continue


if __name__ == "__main__":
    sync_fbo_supplies(limit=50, days_back=30, dry_run=DRY_RUN_FBO)
