# sync_orders.py
import os
import sys
import traceback
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from ms_client import (
    get_organization_by_name,
    get_store_by_name,
    get_or_create_counterparty,
    get_ms_product_by_code,
    create_customer_order,
    find_customer_order_by_name,
    get_customer_order_positions,
    get_demand_by_customer_order,
    create_demand_from_order,
)
from ozon_client import (
    get_new_orders as get_new_orders_ozon1,
    get_fbs_shipments as get_fbs_shipments_ozon1,
)
from ozon_client_trail import (
    get_new_orders as get_new_orders_ozon2,
    get_fbs_shipments as get_fbs_shipments_ozon2,
)
from telegram_logger import log_to_telegram, log_exception_to_telegram

load_dotenv()

DRY_RUN = os.getenv("ORDERS_DRY_RUN", "false").lower() == "true"

ORGANIZATION_NAME = os.getenv("MS_ORGANIZATION_NAME", "ИП Комарицкий Д.С.")
STORE_NAME = os.getenv("MS_STORE_NAME", "Основной склад")

MS_TIMEZONE = timezone(timedelta(hours=3))  # Москва


def parse_ozon_datetime(dt_str: str) -> datetime:
    """
    Парсим дату-время из Ozon, возвращаем datetime в таймзоне МСК.
    """
    # Форматы вида: "2025-02-21T14:22:33Z" или "2025-02-21T14:22:33+03:00"
    try:
        if dt_str.endswith("Z"):
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(dt_str)
    except Exception:
        return datetime.now(MS_TIMEZONE)

    # Переводим в МСК
    return dt.astimezone(MS_TIMEZONE)


def build_order_name_from_ozon(order):
    """
    Генерируем name для заказа покупателя в МойСклад.
    Можно использовать ozon_order_id или posting_number.
    """
    posting_number = order.get("posting_number")
    if posting_number:
        return posting_number
    return f"ozon-{order.get('order_id')}"


def build_ms_positions_from_ozon(order, account_name: str):
    """
    Создаём список позиций для МойСклад по заказу Ozon.
    account_name нужен, если будут отличия между кабинетами.
    """
    positions = []
    for item in order.get("products", []):
        offer_id = item.get("offer_id")  # наш артикул
        quantity = item.get("quantity", 1)
        price = item.get("price", 0)

        if not offer_id:
            print("[ORDERS] Пропущена позиция без offer_id")
            continue

        ms_assortment = get_ms_product_by_code(offer_id)
        if not ms_assortment:
            print(f"[ORDERS] В МойСклад не найден товар с артикулом {offer_id}")
            continue

        positions.append(
            {
                "assortment": ms_assortment,
                "quantity": quantity,
                "price": int(price * 100),
            }
        )

    return positions


def ensure_ms_base_entities():
    """
    Проверяем, что в МойСклад есть организация и склад.
    """
    org = get_organization_by_name(ORGANIZATION_NAME)
    if not org:
        raise RuntimeError(f"Организация '{ORGANIZATION_NAME}' не найдена в МойСклад")

    store = get_store_by_name(STORE_NAME)
    if not store:
        raise RuntimeError(f"Склад '{STORE_NAME}' не найден в МойСклад")

    return org, store


def process_ozon_orders(ozon_orders: list[dict], account_name: str):
    """
    Создание/обновление заказов покупателей по новым заказам Ozon.
    """
    print(f"[ORDERS] Аккаунт={account_name}, получено заказов: {len(ozon_orders)}, DRY_RUN={DRY_RUN}")

    org, store = ensure_ms_base_entities()

    for order in ozon_orders:
        try:
            posting_number = order.get("posting_number")
            status = order.get("status")
            print(
                f"[ORDERS] Обработка заказа {posting_number} "
                f"(аккаунт={account_name}, статус={status}), DRY_RUN={DRY_RUN}"
            )

            # Покупатель
            buyer_name = order.get("customer", {}).get("name") or "Покупатель Ozon"
            buyer_phone = order.get("customer", {}).get("phone")
            buyer_email = order.get("customer", {}).get("email")

            counterparty = get_or_create_counterparty(
                name=buyer_name,
                inn=None,
                phone=buyer_phone,
                email=buyer_email,
            )

            # Позиции
            positions = build_ms_positions_from_ozon(order, account_name)

            # Имя заказа
            ms_order_name = build_order_name_from_ozon(order)

            # Проверим, есть ли уже такой заказ в МойСклад
            existing = find_customer_order_by_name(ms_order_name)
            if existing:
                print(f"[ORDERS] Заказ {ms_order_name} уже существует в МойСклад.")
                if not DRY_RUN:
                    # При желании можно обновлять позиции:
                    # existing_href = existing["meta"]["href"]
                    # update_customer_order_positions(existing_href, positions)
                    pass
                continue

            if DRY_RUN:
                print(f"[ORDERS] DRY_RUN: заказ {ms_order_name} не будет создан.")
                continue

            # Создаём заказ в МойСклад
            created = create_customer_order(
                organization=org,
                agent=counterparty,
                store=store,
                name=ms_order_name,
                positions=positions,
                description=f"Ozon заказ {posting_number} (аккаунт: {account_name})",
            )
            print(f"[ORDERS] Создан заказ {created.get('name')} в МойСклад.")

        except Exception as e:
            tb = traceback.format_exc()
            print(f"[ORDERS] Ошибка при обработке заказа Ozon: {e}\n{tb}")
            log_exception_to_telegram(
                f"Ошибка при обработке заказа Ozon (аккаунт={account_name}, posting={order.get('posting_number')}): {e}"
            )


def process_ozon_shipments(ozon_shipments: list[dict], account_name: str):
    """
    Обработка FBS-отправлений Ozon: создание отгрузок (demand) по существующим заказам покупателей.
    """
    print(f"[ORDERS] Аккаунт={account_name}, получено отправлений: {len(ozon_shipments)}, DRY_RUN={DRY_RUN}")

    for shipment in ozon_shipments:
        posting_number = shipment.get("posting_number")
        status = shipment.get("status")
        products = shipment.get("products", [])
        print(
            f"[ORDERS] Обработка отправления {posting_number} "
            f"(аккаунт={account_name}, статус={status}), позиций: {len(products)}, DRY_RUN={DRY_RUN}"
        )

        try:
            # Ищем заказ в МойСклад по имени, совпадающему с posting_number
            ms_order = find_customer_order_by_name(posting_number)
            if not ms_order:
                print(f"[ORDERS] В МойСклад не найден заказ с именем {posting_number}. Пропуск.")
                continue

            order_href = ms_order["meta"]["href"]

            # Проверяем, есть ли уже отгрузка по этому заказу
            existing_demand = get_demand_by_customer_order(order_href)
            if existing_demand:
                print(f"[ORDERS] Отгрузка по заказу {posting_number} уже существует в МойСклад.")
                continue

            if DRY_RUN:
                print(f"[ORDERS] DRY_RUN: отгрузка по заказу {posting_number} создаваться не будет.")
                continue

            # Создаем отгрузку на основании заказа покупателя
            print(f"[ORDERS] Создание отгрузки по существующему заказу {posting_number}...")
            try:
                created_demand = create_demand_from_order(order_href)
                print(
                    f"[ORDERS] Создана отгрузка {created_demand.get('name')} "
                    f"по заказу {posting_number} в МойСклад."
                )
            except Exception as e_demand:
                print(
                    f"[ORDERS] Ошибка создания отгрузки для существующего заказа {posting_number}: {e_demand}"
                )
                log_exception_to_telegram(
                    f"Ошибка создания отгрузки для существующего заказа {posting_number} (аккаунт={account_name}): {e_demand}"
                )
                continue

        except Exception as e:
            tb = traceback.format_exc()
            print(
                f"[ORDERS] Ошибка по отправлению {posting_number} ({account_name}): {e}\n{tb}"
            )
            log_exception_to_telegram(
                f"Ошибка по отправлению {posting_number} ({account_name}): {e}"
            )


def main():
    print("Запуск синхронизации заказов Ozon с МойСклад...")

    try:
        # Новый период: последние 7 дней
        date_from = datetime.now(tz=MS_TIMEZONE) - timedelta(days=7)

        # --- Ozon 1 (Auto-MiX) ---
        ozon1_orders = get_new_orders_ozon1(date_from=date_from)
        ozon1_shipments = get_fbs_shipments_ozon1(date_from=date_from)

        # --- Ozon 2 (Trail Gear) ---
        ozon2_orders = get_new_orders_ozon2(date_from=date_from)
        ozon2_shipments = get_fbs_shipments_ozon2(date_from=date_from)

        # Обработка заказов
        process_ozon_orders(ozon1_orders, account_name="ozon1")
        process_ozon_orders(ozon2_orders, account_name="ozon2")

        # Обработка FBS-отправлений (создание отгрузок)
        process_ozon_shipments(ozon1_shipments, account_name="ozon1")
        process_ozon_shipments(ozon2_shipments, account_name="ozon2")

        log_to_telegram("Синхронизация заказов и отгрузок Ozon ↔ МойСклад завершена.")

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[ORDERS] Общая ошибка при синхронизации: {e}\n{tb}")
        log_exception_to_telegram(f"Общая ошибка при синхронизации заказов Ozon: {e}")


if __name__ == "__main__":
    main()
