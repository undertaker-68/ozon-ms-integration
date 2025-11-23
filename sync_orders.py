import json
import os

from dotenv import load_dotenv

from ozon_client import get_fbs_postings
from ms_client import (
    find_product_by_article,
    create_customer_order,
    find_customer_order_by_name,
    update_customer_order_state,
    clear_reserve_for_order,
    create_demand_from_order,
)

load_dotenv()

# Статусы МойСклад из .env
MS_STATE_AWAITING_PACKAGING = os.getenv("MS_STATE_AWAITING_PACKAGING")
MS_STATE_AWAITING_SHIPMENT = os.getenv("MS_STATE_AWAITING_SHIPMENT")
MS_STATE_DELIVERING = os.getenv("MS_STATE_DELIVERING")
MS_STATE_CANCELLED = os.getenv("MS_STATE_CANCELLED")
MS_STATE_DELIVERED = os.getenv("MS_STATE_DELIVERED")

# DRY RUN режима
DRY_RUN_ORDERS = os.getenv("DRY_RUN_ORDERS", "true").lower() == "true"


def build_customer_order_payload(posting: dict, ms_positions: list) -> dict:
    """
    Формируем черновик заказа покупателя.
    """
    posting_number = posting.get("posting_number", "NO_NUMBER")

    payload = {
        "name": f"OZON-{posting_number}",
        "description": "Заказ из Ozon (dry-run, пока без создания в МойСклад)",
        "organization": {
            "meta": {
                "href": "https://api.moysklad.ru/api/remap/1.2/entity/organization/4116ceb4-6f3d-11eb-0a80-007800235ec3"
            }
        },
        "agent": {
            "meta": {
                "href": "https://api.moysklad.ru/api/remap/1.2/entity/counterparty/0da0f1f4-c762-11f0-0a80-1b110015ba01"
            }
        },
        "store": {
            "meta": {
                "href": "https://api.moysklad.ru/api/remap/1.2/entity/store/03ade8fe-c762-11f0-0a80-19c80015d83e"
            }
        },
        "positions": [],
    }

    for pos in ms_positions:
        payload["positions"].append({
            "quantity": pos["quantity"],
            "assortment": {"meta": pos["ms_meta"]},
            "reserve": pos["quantity"],
        })

    return payload


def sync_fbs_orders(dry_run: bool = True, limit: int = 3):
    data = get_fbs_postings(limit=limit)

    postings = data.get("result", {}).get("postings", [])

    if not postings:
        print("Нет отправлений Ozon по фильтру.")
        return

    print(f"Найдено {len(postings)} отправлений (покажем до {limit}).")

    for posting in postings[:limit]:
        posting_number = posting.get("posting_number")
        status = posting.get("status")
        products = posting.get("products", [])

        print(f"\n=== Обработка отправления {posting_number} ===")

        ms_positions = []
        for p in products:
            article = p.get("offer_id")
            quantity = p.get("quantity", 0)

            if not article:
                print("  Пропущен товар без article")
                continue

            ms_item = find_product_by_article(article)
            if not ms_item:
                print(f"  Не найден товар в МойСклад по артикулу {article}")
                continue

            ms_positions.append({
                "article": article,
                "quantity": quantity,
                "ms_meta": ms_item["meta"],
            })

        if not ms_positions:
            print("  Нет сопоставленных товаров.")
            continue

        # Собираем тело заказа
        order_payload = build_customer_order_payload(posting, ms_positions)
        order_name = order_payload["name"]

        print("  СФОРМИРОВАН ЗАКАЗ ДЛЯ МС (dry-run):")
        print(json.dumps(order_payload, ensure_ascii=False, indent=2))

        print(f"  Статус отправления в Ozon: {status}")

        # ========== ЛОГИКА СТАТУСОВ ==========

        # 1 — Новый заказ: awaiting_packaging
        if status == "awaiting_packaging":
            print(f"  → ЛОГИКА: создать заказ {order_name}, статус 'Ожидают сборки', резервировать позиции.")

            if not dry_run:
                existing = find_customer_order_by_name(order_name)
                if existing:
                    order_href = existing["meta"]["href"]
                else:
                    created = create_customer_order(order_payload)
                    order_href = created["meta"]["href"]

                if MS_STATE_AWAITING_PACKAGING:
                    update_customer_order_state(order_href, MS_STATE_AWAITING_PACKAGING)

        # 2 — Ozon: awaiting_deliver → МС: Собран (Ожидают отгрузки)
        elif status == "awaiting_deliver":
            print(f"  → ЛОГИКА: перевести заказ {order_name} в статус 'Ожидают отгрузки' (резерв остаётся).")

            if not dry_run:
                existing = find_customer_order_by_name(order_name)
                if existing:
                    order_href = existing["meta"]["href"]
                    if MS_STATE_AWAITING_SHIPMENT:
                        update_customer_order_state(order_href, MS_STATE_AWAITING_SHIPMENT)

        # 3 — Доставляются
        elif status == "delivering":
            print(f"  → ЛОГИКА: {order_name} → статус 'Доставляются', снять резерв, создать Отгрузку.")

            if not dry_run:
                existing = find_customer_order_by_name(order_name)
                if existing:
                    order_href = existing["meta"]["href"]

                    if MS_STATE_DELIVERING:
                        update_customer_order_state(order_href, MS_STATE_DELIVERING)

                    clear_reserve_for_order(order_href)
                    create_demand_from_order(order_href)

        # 4 — Отменён
        elif status == "cancelled":
            print(f"  → ЛОГИКА: {order_name} → статус 'Отменен', снять резерв.")

            if not dry_run:
                existing = find_customer_order_by_name(order_name)
                if existing:
                    order_href = existing["meta"]["href"]

                    if MS_STATE_CANCELLED:
                        update_customer_order_state(order_href, MS_STATE_CANCELLED)

                    clear_reserve_for_order(order_href)

        # 5 — Доставлен
        elif status == "delivered":
            print(f"  → ЛОГИКА: {order_name} → статус 'Доставлен' / 'Завершен'.")

            if not dry_run and MS_STATE_DELIVERED:
                existing = find_customer_order_by_name(order_name)
                if existing:
                    order_href = existing["meta"]["href"]
                    update_customer_order_state(order_href, MS_STATE_DELIVERED)

        # 6 — все остальные статусы
        else:
            print("  → ЛОГИКА: Статус не обработан, просто выводим информацию.")

        if not dry_run:
            print("  (БОЕВОЙ РЕЖИМ: действия выполнены)")


if __name__ == "__main__":
    sync_fbs_orders(dry_run=DRY_RUN_ORDERS, limit=3)
