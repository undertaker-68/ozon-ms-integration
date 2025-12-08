import os
import time
from dotenv import load_dotenv

from ms_client import (
    find_product_by_article,
    find_counterparty_by_name_or_phone,
    create_customer_order,
    update_customer_order_state,
    create_demand_from_order,
)

from ozon_client import get_failed_orders, get_new_orders as get_new_orders_ozon1
from ozon_client2 import get_new_orders as get_new_orders_ozon2

from notifier import send_telegram_message

load_dotenv()

# -----------------------------
# КАНАЛЫ ПРОДАЖ (Оба кабинета)
# -----------------------------

SALES_CHANNEL_AUTOMIX = {
    "meta": {
        "href": "https://api.moysklad.ru/api/remap/1.2/entity/saleschannel/fede2826-9fd0-11ee-0a80-0641000f3d25",
        "type": "saleschannel",
        "mediaType": "application/json",
    }
}

SALES_CHANNEL_TRAIL = {
    "meta": {
        "href": "https://api.moysklad.ru/api/remap/1.2/entity/saleschannel/ff2827b8-9fd0-11ee-0a80-0641000f3d31",
        "type": "saleschannel",
        "mediaType": "application/json",
    }
}

# -----------------------------
# ОСНОВНАЯ ЛОГИКА
# -----------------------------

def build_ms_order_payload(order, sales_channel_meta):
    """
    Формирует payload для создания заказа в МойСклад.
    Добавлен параметр sales_channel_meta — правильный канал продаж.
    """
    customer = order.get("customer")
    buyer_name = customer.get("name") if customer else ""
    phone = customer.get("phone") if customer else ""

    counterparty = None
    if phone:
        counterparty = find_counterparty_by_name_or_phone(phone)
    if not counterparty and buyer_name:
        counterparty = find_counterparty_by_name_or_phone(buyer_name)

    if counterparty:
        counterparty_meta = counterparty.get("meta")
    else:
        # если не найден — создаём контрагента в MS
        counterparty_meta = {
            "href": None,
            "type": "counterparty"
        }

    # позиции заказа
    positions = []
    for item in order.get("items", []):
        article = item.get("offer_id")
        qty = item.get("quantity", 1)

        ms_product = find_product_by_article(article)
        if not ms_product:
            continue

        positions.append(
            {
                "quantity": qty,
                "assortment": ms_product.get("meta"),
            }
        )

    payload = {
        "name": str(order.get("order_id")),
        "description": f"Заказ с Ozon #{order.get('order_id')}",
        "organization": None,
        "agent": counterparty_meta,
        "positions": positions,
        # ←–––––––––––––––––––– добавили канал продаж
        "salesChannel": sales_channel_meta,
    }

    return payload


def process_order(order, from_cabinet):
    """
    Обработка заказа из одного из кабинетов Ozon.

    from_cabinet = 1 → Auto-MiX
    from_cabinet = 2 → Trail Gear
    """
    order_id = order.get("order_id")

    try:
        # Выбираем канал продаж под кабинет
        if from_cabinet == 1:
            channel_meta = SALES_CHANNEL_AUTOMIX
        else:
            channel_meta = SALES_CHANNEL_TRAIL

        payload = build_ms_order_payload(order, channel_meta)

        ms_order = create_customer_order(payload)

        # Меняем состояние заказа (если настроено)
        state_href = os.getenv("MS_CUSTOMER_ORDER_STATE_NEW")
        if state_href:
            update_customer_order_state(ms_order["meta"]["href"], state_href)

        # Создаём отгрузку
        demand = create_demand_from_order(ms_order)

        print(
            f"[OK] Заказ {order_id} из кабинета {from_cabinet} создан в МС. "
            f"Отгрузка: {demand.get('name')}"
        )

    except Exception as exc:
        print(f"[ERROR] Не удалось обработать заказ {order_id}: {exc}")
        try:
            send_telegram_message(f"Ошибка обработки заказа {order_id}: {exc}")
        except:
            pass


def main_loop():
    print("=== Запуск синхронизации заказов Ozon → МойСклад ===")

    while True:
        try:
            # Кабинет 1 (Auto-MiX)
            orders1 = get_new_orders_ozon1()
            for order in orders1:
                process_order(order, from_cabinet=1)

            # Кабинет 2 (Trail Gear)
            orders2 = get_new_orders_ozon2()
            for order in orders2:
                process_order(order, from_cabinet=2)

        except Exception as exc:
            print(f"[FATAL] Ошибка цикла: {exc}")
            try:
                send_telegram_message(f"Ошибка цикла заказов: {exc}")
            except:
                pass

        # Период опроса
        time.sleep(15)


if __name__ == "__main__":
    main_loop()
