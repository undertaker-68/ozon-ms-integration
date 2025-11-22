import json
import os

from dotenv import load_dotenv

from ozon_client import get_fbs_postings
from ms_client import find_product_by_article

load_dotenv()

# Флаг "боевого режима" для заказов.
# Пока оставляем только dry-run, потом можно будет вынести в .env
DRY_RUN_ORDERS = os.getenv("DRY_RUN_ORDERS", "true").lower() == "true"


def build_customer_order_payload(posting: dict, ms_positions: list) -> dict:
    """
    Сконструировать тело заказа покупателя МойСклад (черновик).

    posting      — объект отправления из Ozon (один элемент из result.postings)
    ms_positions — список найденных позиций в МойСклад (с метой и количеством)
    """
    posting_number = posting.get("posting_number", "NO_NUMBER")

    payload = {
        "name": f"OZON-{posting_number}",
        "description": "Заказ из Ozon (dry-run, пока без создания в МойСклад)",
        # Здесь уже подставлены реальные meta.href из твоего аккаунта
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
            "assortment": {
                "meta": pos["ms_meta"],
            },
            "reserve": pos["quantity"],  # сразу резервируем в заказе
        })

    return payload


def sync_fbs_orders(dry_run: bool = True, limit: int = 3):
    """
    Берём несколько FBS-отправлений из Ozon и по каждому
    строим проект заказа покупателя для МойСклад.

    Сейчас: НИЧЕГО НЕ СОЗДАЁМ, только печатаем:
      - черновик заказа МС
      - статус Ozon
      - какую логику применили бы в МС
    """
    data = get_fbs_postings(limit=limit)

    result = data.get("result") or {}
    postings = result.get("postings", [])

    if not postings:
        print("В Ozon не найдено отправлений по заданному фильтру.")
        return

    print(f"Найдено {len(postings)} отправлений (покажем до {limit}).")

    for posting in postings[:limit]:
        posting_number = posting.get("posting_number")
        products = posting.get("products", [])
        status = posting.get("status")

        print(f"\n=== Обработка отправления {posting_number} ===")

        ms_positions = []

        for p in products:
            offer_id = p.get("offer_id")
            quantity = p.get("quantity", 0)

            if not offer_id:
                print("  Пропуск товара без offer_id")
                continue

            ms_product = find_product_by_article(offer_id)

            if not ms_product:
                print(f"  Не найден товар в МойСклад по артикулу {offer_id}")
                continue

            ms_positions.append({
                "article": offer_id,
                "ms_name": ms_product.get("name"),
                "ms_meta": ms_product.get("meta"),  # meta товара в МС
                "quantity": quantity,
            })

        if not ms_positions:
            print("  Нет ни одной позиции, которую удалось сопоставить с МойСклад.")
            continue

        # Формируем черновик заказа МойСклад
        order_payload = build_customer_order_payload(posting, ms_positions)

        print("  СФОРМИРОВАН ЗАКАЗ ДЛЯ МОЙСКЛАД (dry-run):")
        print(json.dumps(order_payload, ensure_ascii=False, indent=2))

        print(f"  Статус отправления в Ozon: {status}")
        order_name = order_payload.get("name")

        # === ТВОИ ПРАВИЛА ПО СТАТУСАМ ===

        # 1. Новые заказы Ozon → в МС статус "Ожидают сборки", заказ резервируется.
        # Считаем, что это статус awaiting_packaging.
        if status == "awaiting_packaging":
            print("  → ЛОГИКА: создал бы в МойСклад заказ с именем "
                  f"{order_name} со статусом 'Ожидают сборки' и зарезервировал товары.")

        # 2. Ozon: "Ожидают отгрузки" → МС: "Ожидают отгрузки"
        # В Ozon это статус awaiting_deliver.
        elif status == "awaiting_deliver":
            print("  → ЛОГИКА: нашёл бы заказ в МойСклад по имени "
                  f"{order_name} и перевёл статус на 'Ожидают отгрузки'.")
            print("    Резерв оставляем, товары уже собраны и ждут отправки.")

        # 3. Ozon: "Доставляются" → МС: "Доставляются", снять резерв и создать отгрузку
        elif status == "delivering":
            print("  → ЛОГИКА: нашёл бы заказ в МойСклад по имени "
                  f"{order_name}, перевёл статус на 'Доставляются',")
            print("    снял бы резерв по всем позициям и создал документ 'Отгрузка' по этому заказу.")

        # 4. Ozon: "Отменён" → МС: "Отменен" и снять резерв
        elif status == "cancelled":
            print("  → ЛОГИКА: нашёл бы заказ в МойСклад по имени "
                  f"{order_name}, перевёл статус на 'Отменен' и снял резерв по всем позициям.")

        # Дополнительно: доставлен
        elif status == "delivered":
            print("  → ЛОГИКА: заказ доставлен. В МойСклад можно оставить финальный статус,"
                  " например 'Завершен', или не менять, в зависимости от твоей схемы.")

        # Все остальные статусы
        else:
            print("  → ЛОГИКА: для этого статуса пока нет отдельной обработки,"
                  " просто отображаем заказ.")

        if not dry_run:
            # Здесь в будущем будет реальный вызов функций из ms_client:
            #   - create/update customerorder
            #   - изменение статуса (state)
            #   - создание отгрузки (demand)
            print("  (боевой режим пока не включен, реально в МойСклад НИЧЕГО не отправляется)")


if __name__ == "__main__":
    # ЖЁСТКО оставляем dry-run, пока ты не решишь включать боевой режим
    sync_fbs_orders(dry_run=DRY_RUN_ORDERS, limit=3)
