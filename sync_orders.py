import json

from dotenv import load_dotenv

from ozon_client import get_fbs_postings
from ms_client import find_product_by_article

load_dotenv()


def build_customer_order_payload(posting: dict, ms_positions: list) -> dict:
    """
    Сконструировать тело заказа покупателя МойСклад (пока в виде черновика).

    posting      — объект отправления из Ozon (один элемент из result.postings)
    ms_positions — список найденных позиций в МойСклад (уже с артикулом, именем, qty)
    """
    posting_number = posting.get("posting_number", "NO_NUMBER")

    payload = {
        "name": f"OZON-{posting_number}",
        "description": "Заказ из Ozon (dry-run, пока без создания в МойСклад)",
        # Эти поля позже заменим на реальные meta-объекты организации/контрагента/склада:
        "organization": {"meta": {"href": "https://api.moysklad.ru/api/remap/1.2/entity/organization/XXX"}},
        "agent": {"meta": {"href": "https://api.moysklad.ru/api/remap/1.2/entity/counterparty/YYY"}},
        "store": {"meta": {"href": "https://api.moysklad.ru/api/remap/1.2/entity/store/ZZZ"}},
        "positions": [],
    }

    for pos in ms_positions:
        payload["positions"].append({
            # пока без реального meta товара и цен, только скелет
            "quantity": pos["quantity"],
            "assortment": {
                # сюда позже подставим ms_product["meta"]
                "meta": pos["ms_meta"],
            },
            "reserve": pos["quantity"],  # сразу резервируем в заказе
        })

    return payload


def sync_fbs_orders(dry_run: bool = True, limit: int = 3):
    """
    Берём несколько FBS-отправлений из Ozon и по каждому
    строим проект заказа покупателя для МойСклад.
    НИЧЕГО НЕ СОЗДАЁМ, только печатаем JSON.
    """
    data = get_fbs_postings(limit=limit)

    result = data.get("result") or data.get("result", {})
    postings = result.get("postings", []) if isinstance(result, dict) else []

    if not postings:
        print("В Ozon не найдено отправлений по заданному фильтру.")
        return

    print(f"Найдено {len(postings)} отправлений (покажем до {limit}).")

    for posting in postings[:limit]:
        posting_number = posting.get("posting_number")
        products = posting.get("products", [])

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
                "ms_meta": ms_product.get("meta"),  # пригодится для реального заказа
                "quantity": quantity,
            })

        if not ms_positions:
            print("  Нет ни одной позиции, которую удалось сопоставить с МойСклад.")
            continue

        order_payload = build_customer_order_payload(posting, ms_positions)

        print("  СФОРМИРОВАН ЗАКАЗ ДЛЯ МОЙСКЛАД (dry-run):")
        print(json.dumps(order_payload, ensure_ascii=False, indent=2))

        if not dry_run:
            # Здесь в будущем будет реальный вызов:
            #   ms_client.create_customer_order(order_payload)
            # Сейчас — только dry-run.
            print("  (боевой режим ещё не реализован, dry_run принудительно включен)")


if __name__ == "__main__":
    # ЖЁСТКО оставляем dry_run=True, чтобы случайно ничего не создать
    sync_fbs_orders(dry_run=True, limit=3)
