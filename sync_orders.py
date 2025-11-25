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
from notifier import send_telegram_message

load_dotenv()

DRY_RUN_ORDERS = os.getenv("DRY_RUN_ORDERS", "true").lower() == "true"

# meta.href нужных статусов заказа в МойСклад
MS_STATE_AWAIT_PACK = os.getenv("MS_STATE_AWAIT_PACK")      # Ожидают сборки
MS_STATE_AWAIT_SHIP = os.getenv("MS_STATE_AWAIT_SHIP")      # Ожидают отгрузки
MS_STATE_DELIVERING = os.getenv("MS_STATE_DELIVERING")      # Доставляются
MS_STATE_DELIVERED = os.getenv("MS_STATE_DELIVERED")        # Доставлен
MS_STATE_CANCELLED = os.getenv("MS_STATE_CANCELLED")        # Отменён/Закрыт


def build_ms_positions_from_posting(posting: dict) -> list[dict]:
    """
    Для одного отправления Ozon строим список позиций МС:
    [{'quantity': X, 'ms_meta': {...}}, ...]
    Если хотя бы один товар не найден — возвращаем пустой список.
    """
    products = posting.get("products") or []
    ms_positions = []
    missing = []

    for p in products:
        offer_id = p.get("offer_id")
        qty = p.get("quantity") or 0
        if not offer_id or not qty:
            continue

        ms_product = find_product_by_article(offer_id)
        if not ms_product:
            missing.append(offer_id)
            continue

        ms_positions.append(
            {
                "quantity": qty,
                "ms_meta": ms_product["meta"],
            }
        )

    if missing:
        text = (
            "❗ Не найден(ы) товар(ы) в МойСклад по артикулу из Ozon\n"
            f"Отправление: {posting.get('posting_number')}\n"
            f"Артикулы: {', '.join(missing)}"
        )
        print("[ORDERS]", text.replace("\n", " | "))
        try:
            send_telegram_message(text)
        except Exception:
            pass
        return []

    return ms_positions


def build_customer_order_payload(posting: dict, ms_positions: list) -> dict:
    posting_number = posting.get("posting_number", "NO_NUMBER")

    payload = {
        "name": f"OZON-{posting_number}",
        "description": "Заказ из Ozon (создан скриптом интеграции)",
        "organization": {
            "meta": {
                "href": "https://api.moysklad.ru/api/remap/1.2/entity/organization/4116ceb4-6f3d-11eb-0a80-007800235ec3",
                "type": "organization",
                "mediaType": "application/json",
            }
        },
        "agent": {
            "meta": {
                "href": "https://api.moysklad.ru/api/remap/1.2/entity/counterparty/f61bfcf9-2d74-11ec-0a80-04c700041e03",
                "type": "counterparty",
                "mediaType": "application/json",
            }
        },
        "store": {
            "meta": {
                "href": "https://api.moysklad.ru/api/remap/1.2/entity/store/03ade8fe-c762-11f0-0a80-19c80015d83e",
                "type": "store",
                "mediaType": "application/json",
            }
        },
        "positions": [],
    }

    for pos in ms_positions:
        payload["positions"].append(
            {
                "quantity": pos["quantity"],
                "assortment": {"meta": pos["ms_meta"]},
                "reserve": pos["quantity"],
            }
        )

    return payload


def process_posting(posting: dict, dry_run: bool) -> None:
    posting_number = posting.get("posting_number")
    status = posting.get("status")
    print(f"[ORDERS] Обработка отправления {posting_number}, статус Ozon: {status}")

    ms_positions = build_ms_positions_from_posting(posting)
    if not ms_positions:
        print(f"[ORDERS] Пропускаем {posting_number}: нет ни одной позиции в МС")
        return

    order_name = f"OZON-{posting_number}"
    existing_order = find_customer_order_by_name(order_name)

    if status == "awaiting_packaging":
        # Создать заказ, статус "Ожидают сборки", поставить резерв
        if existing_order:
            print(f"[ORDERS] Заказ {order_name} уже существует, повторно не создаём.")
            return

        order_payload = build_customer_order_payload(posting, ms_positions)
        print(f"[ORDERS] Создание заказа {order_name} (awaiting_packaging)")

        if dry_run:
            print("[ORDERS] DRY_RUN_ORDERS=TRUE: заказ не создаётся в МС.")
            return

        created = create_customer_order(order_payload)
        if MS_STATE_AWAIT_PACK:
            update_customer_order_state(created["meta"]["href"], MS_STATE_AWAIT_PACK)
        print(f"[ORDERS] Заказ {order_name} создан, статус 'Ожидают сборки'.")

    elif status == "awaiting_deliver":
        # Заказ уже должен существовать, переводим в "Ожидают отгрузки"
        if not existing_order:
            print(f"[ORDERS] {order_name}: заказ не найден в МС, создать можно при необходимости.")
            if dry_run:
                return
            order_payload = build_customer_order_payload(posting, ms_positions)
            created = create_customer_order(order_payload)
            existing_order = created

        print(f"[ORDERS] Перевод заказа {order_name} в 'Ожидают отгрузки'")
        if dry_run:
            print("[ORDERS] DRY_RUN_ORDERS=TRUE: статус в МС не меняем.")
            return

        if MS_STATE_AWAIT_SHIP:
            update_customer_order_state(existing_order["meta"]["href"], MS_STATE_AWAIT_SHIP)
        print(f"[ORDERS] Заказ {order_name}: статус обновлён на 'Ожидают отгрузки'.")

    elif status == "delivering":
        # Заказ в доставке: статус "Доставляются", снять резерв, создать Отгрузку
        if not existing_order:
            print(f"[ORDERS] {order_name}: заказ не найден, создаём перед отгрузкой.")
            if dry_run:
                print("[ORDERS] DRY_RUN_ORDERS=TRUE: создание заказа пропущено.")
                return
            order_payload = build_customer_order_payload(posting, ms_positions)
            created = create_customer_order(order_payload)
            existing_order = created

        href = existing_order["meta"]["href"]
        print(f"[ORDERS] Обновление {order_name}: статус 'Доставляются', снятие резерва, создание отгрузки.")
        if dry_run:
            print("[ORDERS] DRY_RUN_ORDERS=TRUE: изменения в МС не выполняются.")
            return

        if MS_STATE_DELIVERING:
            update_customer_order_state(href, MS_STATE_DELIVERING)
        clear_reserve_for_order(href)
        create_demand_from_order(href)
        print(f"[ORDERS] За {order_name}: резерв снят, отгрузка создана.")

    elif status == "cancelled":
        # Отмена: по желанию можно снять резерв и поставить статус "Отменён"
        if not existing_order:
            print(f"[ORDERS] {order_name}: нет заказа в МС, нечего отменять.")
            return

        href = existing_order["meta"]["href"]
        print(f"[ORDERS] Отмена заказа {order_name}.")
        if dry_run:
            print("[ORDERS] DRY_RUN_ORDERS=TRUE: отмена не выполняется.")
            return

        clear_reserve_for_order(href)
        if MS_STATE_CANCELLED:
            update_customer_order_state(href, MS_STATE_CANCELLED)
        print(f"[ORDERS] Заказ {order_name}: резерв снят, статус 'Отменён'.")

    elif status == "delivered":
        # Доставлен: можно перевести в "Доставлен", убедиться, что резерв снят
        if not existing_order:
            print(f"[ORDERS] {order_name}: нет заказа в МС, статус delivered игнорируем.")
            return

        href = existing_order["meta"]["href"]
        print(f"[ORDERS] Заказ {order_name} доставлен, обновляем статус.")
        if dry_run:
            print("[ORDERS] DRY_RUN_ORDERS=TRUE: статус не меняем.")
            return

        clear_reserve_for_order(href)
        if MS_STATE_DELIVERED:
            update_customer_order_state(href, MS_STATE_DELIVERED)
        print(f"[ORDERS] Заказ {order_name}: статус 'Доставлен', резерв снят.")

    else:
        print(f"[ORDERS] Статус {status} пока не обрабатывается, {posting_number} пропущен.")


def sync_fbs_orders(dry_run: bool, limit: int = 3) -> None:
    print(f"[ORDERS] Старт sync_fbs_orders, DRY_RUN_ORDERS={dry_run}")
    data = get_fbs_postings(limit=limit)
    postings = data.get("result", {}).get("postings", [])
    print(f"[ORDERS] Найдено отправлений: {len(postings)}")

    for p in postings:
        try:
            process_posting(p, dry_run=dry_run)
        except Exception as e:
            msg = (
                "❗ Ошибка обработки отправления Ozon\n"
                f"posting_number: {p.get('posting_number')}\n"
                f"error: {e!r}"
            )
            print("[ORDERS]", msg.replace("\n", " | "))
            try:
                send_telegram_message(msg)
            except Exception:
                pass


if __name__ == "__main__":
    sync_fbs_orders(dry_run=DRY_RUN_ORDERS, limit=3)
