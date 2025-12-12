import os
import json
import time
from datetime import datetime, timezone
from typing import List, Dict, Any

from dotenv import load_dotenv

from ozon_fbo_client import OzonFboClient
from ms_client import (
    find_customer_order_by_name,
    create_customer_order,
    update_customer_order,
)

load_dotenv()

DRY_RUN_FBO = os.getenv("DRY_RUN_FBO", "false").lower() == "true"

# ==========================
# НАСТРОЙКИ
# ==========================

# ВРЕМЕННО: работаем только с этими поставками
ONLY_ORDER_NUMBERS = {
    "2000037545485",
    "2000037485754",
}

# Только эти статусы считаем «Подготовка к поставкам»
ALLOWED_OZON_STATES = {
    "DATA_FILLING",
    "READY_TO_SUPPLY",
}

# ==========================
# ВСПОМОГАТЕЛЬНОЕ
# ==========================

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _safe_int(v):
    try:
        return int(v)
    except Exception:
        return None


# ==========================
# ОСНОВНАЯ ЛОГИКА
# ==========================

def _process_single_fbo_order(order: Dict[str, Any], client: OzonFboClient, dry_run: bool) -> None:
    order_number = str(order.get("order_number") or "")
    state = order.get("state")

    if order_number not in ONLY_ORDER_NUMBERS:
        return

    if state not in ALLOWED_OZON_STATES:
        print(f"[FBO] Пропуск {order_number}: state={state}")
        return

    supplies = order.get("supplies") or []
    if not supplies:
        print(f"[FBO] {order_number}: нет supplies")
        return

    # Берём первый supply (для подготовки он один)
    supply = supplies[0]

    arrival_date = (
        supply.get("storage_warehouse", {})
        .get("arrival_date")
    )

    warehouse_name = (
        supply.get("storage_warehouse", {})
        .get("name")
    )

    cluster = "Москва и МО" if "ПУШКИНО" in (warehouse_name or "") else warehouse_name

    description = f"{order_number} - {cluster}"

    # -------- позиции --------
    positions = []
    for s in supplies:
        bundle_id = s.get("bundle_id")
        if not bundle_id:
            continue

        items = client.get_bundle_items(bundle_id)
        for it in items:
            article = it.get("offer_id") or it.get("article")
            qty = _safe_int(it.get("quantity")) or 0
            if not article or qty <= 0:
                continue

            positions.append({
                "article": str(article),
                "quantity": qty,
            })

    if not positions:
        print(f"[FBO] {order_number}: нет позиций")
        return

    print(
        f"[FBO] Обработка заявки {order_number}: "
        f"state={state}, позиций={len(positions)}, DRY_RUN={dry_run}"
    )

    if dry_run:
        return

    existing = find_customer_order_by_name(order_number)

    payload = {
        "name": order_number,
        "description": description,
        "plannedMoment": arrival_date,
        "positions": positions,
    }

    if existing:
        update_customer_order(existing["meta"]["href"], payload)
        print(f"[FBO] Заказ {order_number} обновлён в МС")
    else:
        create_customer_order(payload)
        print(f"[FBO] Заказ {order_number} создан в МС")


def sync_fbo_supplies(limit: int = 50, days_back: int = 30, dry_run: bool = False) -> None:
    print(
        f"Запуск синхронизации FBO-поставок "
        f"(limit={limit}, days_back={days_back}, DRY_RUN={dry_run})"
    )

    for acc in ("ozon1", "ozon2"):
        cid = os.getenv(f"OZON_{acc.upper()}_CLIENT_ID")
        key = os.getenv(f"OZON_{acc.upper()}_API_KEY")
        if not cid or not key:
            continue

        client = OzonFboClient(cid, key, acc)

        orders = client.list_supply_orders(limit=limit, days_back=days_back)
        print(f"[FBO] Кабинет {acc}: получено заявок: {len(orders)}")

        for order in orders:
            try:
                _process_single_fbo_order(order, client, dry_run=dry_run)
            except Exception as e:
                print(
                    f"[FBO] Ошибка обработки "
                    f"{order.get('order_number')} ({acc}): {e!r}"
                )
                continue


if __name__ == "__main__":
    sync_fbo_supplies(limit=50, days_back=30, dry_run=DRY_RUN_FBO)
