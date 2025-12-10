import os
import csv
from datetime import datetime
import requests
import asyncio
from dotenv import load_dotenv
from ozon_client import get_fbs_postings as get_fbs_postings_ozon1
from ms_client import (
    find_product_by_article,
    create_customer_order,
    find_customer_order_by_name,
    update_customer_order_state,
    clear_reserve_for_order,
    create_demand_from_order,
)
from telegram import Bot

try:
    from notifier import send_telegram_message, send_telegram_document
except ImportError:
    def send_telegram_message(text: str) -> bool:
        print("Telegram notifier не доступен:", text)
        return False

    def send_telegram_document(path: str, caption: str | None = None) -> bool:
        print(f"Telegram document не доступен: {path} {caption}")
        return False


load_dotenv()

# -----------------------------
# Каналы продаж МойСклад для заказов
# -----------------------------

SALES_CHANNEL_AUTOMIX_META = {
    "href": "https://api.moysklad.ru/api/remap/1.2/entity/saleschannel/fede2826-9fd0-11ee-0a80-0641000f3d25",
    "type": "saleschannel",
    "mediaType": "application/json",
}

SALES_CHANNEL_TRAIL_META = {
    "href": "https://api.moysklad.ru/api/remap/1.2/entity/saleschannel/ff2827b8-9fd0-11ee-0a80-0641000f3d31",
    "type": "saleschannel",
    "mediaType": "application/json",
}

DRY_RUN_ORDERS = os.getenv("DRY_RUN_ORDERS", "true").lower() == "true"

MS_BASE_URL = os.getenv("MS_BASE_URL", "https://api.moysklad.ru/api/remap/1.2")
MS_ORGANIZATION_HREF = os.getenv("MS_ORGANIZATION_HREF")
MS_STORE_HREF = os.getenv("MS_STORE_HREF")
MS_AGENT_HREF = os.getenv("MS_AGENT_HREF")

MS_STATE_AWAIT_PACK = os.getenv("MS_STATE_AWAIT_PACK")
MS_STATE_AWAIT_SHIP = os.getenv("MS_STATE_AWAIT_SHIP")
MS_STATE_DELIVERING = os.getenv("MS_STATE_DELIVERING")
MS_STATE_DELIVERED = os.getenv("MS_STATE_DELIVERED")
MS_STATE_CANCELLED = os.getenv("MS_STATE_CANCELLED")

OZON2_ENABLED = os.getenv("ENABLE_OZON2_ORDERS", "true").lower() == "true"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

ERRORS_AUTO_FILE_PATH = "ozon_orders_errors_auto.csv"
ERRORS_TRAIL_FILE_PATH = "ozon_orders_errors_trail.csv"

if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
    bot = Bot(token=TELEGRAM_TOKEN)
else:
    bot = None


def _append_order_errors_to_file(path: str, rows: list[list[str]]) -> None:
    """
    Дописывает строки с ошибками в CSV-файл (UTF-8 с BOM).
    """
    if not rows:
        return

    file_exists = os.path.exists(path)

    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        if not file_exists:
            writer.writerow(
                ["Дата/время", "Ozon аккаунт", "Номер отправления", "Ошибка"]
            )
        for row in rows:
            writer.writerow(row)


def _format_ms_error(e: Exception) -> str:
    """
    Приводим ошибки МойСклад к человеку-понятному виду.
    """
    if isinstance(e, requests.HTTPError):
        r = e.response
        try:
            data = r.json()
        except Exception:
            return f"HTTP {r.status_code}: {r.text[:500]}"

        if r.status_code == 412:
            errors = data.get("errors") or []
            if errors:
                err_msg = errors[0].get("error") or errors[0].get("message") or ""
                if "Нельзя отгрузить товар, которого нет на складе" in err_msg:
                    return (
                        "МойСклад: нельзя отгрузить товар, которого нет на складе "
                        "(остаток по складу Ozon = 0 или меньше)."
                    )
                return f"МойСклад вернул ошибку 412: {err_msg}"

        if isinstance(data, dict) and data.get("errors"):
            parts: list[str] = []
            for err in data["errors"]:
                msg = err.get("error") or err.get("message")
                if msg:
                    parts.append(msg)
            if parts:
                return f"МойСклад: {', '.join(parts)}"

        return f"HTTP {r.status_code}: {r.text[:500]}"

    return repr(e)


def _send_telegram_error(ozon_account: str, posting_number: str, text: str) -> None:
    """
    Ошибка по отправлению — только лог в консоль, без Телеграма.
    CSV уже формируется выше по коду.
    """
    msg = f"[ORDERS] Ошибка по отправлению {posting_number} ({ozon_account}): {text}"
    print(msg)
    # Телеграм здесь специально отключен, чтобы не спамить чат

def _ms_get_state_meta_href(status: str) -> str | None:
    """
    Маппинг статуса Ozon → состояние заказа в МойСклад.
    Берём href'ы из .env:
      MS_STATE_AWAIT_PACK, MS_STATE_AWAIT_SHIP, MS_STATE_DELIVERING,
      MS_STATE_DELIVERED, MS_STATE_CANCELLED
    """
    status_map = {
        "awaiting_packaging": MS_STATE_AWAIT_PACK,
        "awaiting_deliver": MS_STATE_AWAIT_SHIP,
        "delivering": MS_STATE_DELIVERING,
        "delivered": MS_STATE_DELIVERED,
        "cancelled": MS_STATE_CANCELLED,
    }
    return status_map.get(status)

def process_posting(posting: dict, dry_run: bool) -> None:
    """
    Обработка одного FBS-отправления (оба кабинета):
      - создаём/обновляем заказ в МойСклад
      - проставляем статус в МойСклад по статусу Ozon
      - для delivering/delivered создаём отгрузку (не более 1 раза)
    """
    posting_number = posting.get("posting_number")
    status = posting.get("status")
    ozon_account = posting.get("_ozon_account") or "ozon1"

    # Номер заказа в МС = номеру отправления Ozon
    order_name = posting_number or "UNKNOWN"

    # --- Формируем позиции заказа с ценой из МойСклад ---
    items = posting.get("products") or []
    ms_positions: list[dict] = []

    for item in items:
        offer_id = item.get("offer_id")
        quantity = item.get("quantity") or 0
        if not offer_id or quantity <= 0:
            continue

        product = find_product_by_article(offer_id)
        if not product:
            raise ValueError(f"Товар с артикулом {offer_id!r} не найден в МойСклад")

        # Базовая цена продажи из МойСклад (salePrices[0].value)
        price = None
        sale_prices = product.get("salePrices")
        if isinstance(sale_prices, list) and sale_prices:
            first_price = sale_prices[0] or {}
            price = first_price.get("value")

        ms_positions.append(
            {
                "ms_meta": product["meta"],
                "quantity": quantity,
                "price": price,
            }
        )

    if not ms_positions:
        raise ValueError("Не удалось добавить ни одной позиции с товарами МойСклад")

    positions_payload: list[dict] = []
    for pos in ms_positions:
        item_payload = {
            "quantity": pos["quantity"],
            "assortment": {"meta": pos["ms_meta"]},
        }
        if pos.get("price") is not None:
            item_payload["price"] = pos["price"]
        positions_payload.append(item_payload)

    # --- Мета-данные организации, контрагента и склада ---
    org_meta = {
        "href": MS_ORGANIZATION_HREF,
        "type": "organization",
        "mediaType": "application/json",
    }
    agent_meta = {
        "href": MS_AGENT_HREF,
        "type": "counterparty",
        "mediaType": "application/json",
    }
    store_meta = {
        "href": MS_STORE_HREF,
        "type": "store",
        "mediaType": "application/json",
    }

    # --- Комментарий и канал продаж по кабинету ---
    if ozon_account in ("ozon2", "trail_gear"):
        description = "FBS → Trail Gear"
        sales_channel_meta = SALES_CHANNEL_TRAIL_META
    else:
        description = "FBS → Auto-MiX"
        sales_channel_meta = SALES_CHANNEL_AUTOMIX_META

    payload = {
        "name": order_name,
        "organization": {"meta": org_meta},
        "agent": {"meta": agent_meta},
        "store": {"meta": store_meta},
        "positions": positions_payload,
        "description": description,
        "salesChannel": {"meta": sales_channel_meta},
    }

    # --- Статус заказа в МойСклад по статусу отправления Ozon ---
    state_meta_href = _ms_get_state_meta_href(status)
    if state_meta_href:
        payload["state"] = {
            "meta": {
                "href": state_meta_href,
                "type": "state",
                "mediaType": "application/json",
            }
        }

    print(
        f"[ORDERS] Обработка отправления {posting_number} "
        f"(аккаунт={ozon_account}, статус={status}), "
        f"позиций: {len(positions_payload)}, DRY_RUN={dry_run}"
    )

    if dry_run:
        return

    existing = find_customer_order_by_name(order_name)

    # Если заказ уже есть
    if existing:
        if state_meta_href:
            update_customer_order_state(existing["meta"]["href"], state_meta_href)

        # Создаём отгрузку, только если её НЕТ
        if status in ("delivering", "delivered"):
            existing_demand = find_demand_by_name(order_name)
            if not existing_demand:
                create_demand_from_order(existing)
        return

    # Если заказа ещё нет — создаём
    created = create_customer_order(payload)

    # Если статус уже delivering/delivered — создаём отгрузку, но только если её НЕТ
    if status in ("delivering", "delivered"):
        existing_demand = find_demand_by_name(order_name)
        if not existing_demand:
            create_demand_from_order(created)

async def _sync_for_account(
    ozon_account: str,
    dry_run: bool,
    limit: int,
) -> list[list[str]]:
    """
    Синхронизация заказов по одному аккаунту Ozon.
    Возвращает список строк-ошибок для CSV.
    """
    errors: list[list[str]] = []

    if ozon_account == "ozon1":
        from ozon_client import get_fbs_postings as get_postings
    else:
        from ozon_client2 import get_fbs_postings as get_postings

    try:
        data = get_postings(limit=limit)
    except Exception as e:
        err_text = f"Не удалось получить FBS-отправления: {e!r}"
        print(f"[ORDERS] {err_text}")
        try:
            send_telegram_message(f"[ORDERS] {err_text}")
        except Exception:
            pass
        return errors

    postings = (data or {}).get("result", {}).get("postings") or []

    print(
        f"[ORDERS] Аккаунт={ozon_account}, получено отправлений: {len(postings)}, "
        f"DRY_RUN={dry_run}"
    )

    for posting in postings:
        posting["_ozon_account"] = ozon_account
        posting_number = posting.get("posting_number") or "UNKNOWN"

        # --- ОТСЕЧКА по дате создания в ЛК Ozon ---
        created_date_str = posting.get("created")
        created_date = None
        if created_date_str:
            try:
                created_date = datetime.strptime(created_date_str[:10], "%Y-%m-%d")
            except Exception:
                created_date = None

        hard_cutoff = datetime(2025, 12, 2)  # всё, что создано <= 02.12.2025, не синхронизируем

        if created_date and created_date <= hard_cutoff:
            print(
                f"[ORDERS] Аккаунт={ozon_account}, отправление {posting_number} "
                f"создано {created_date_str}, ≤ 02.12.2025 — пропускаем."
            )
            continue
        # --- конец отсечки ---

        try:
            process_posting(posting, dry_run=dry_run)
        except Exception as e:
            err_text = _format_ms_error(e)
            errors.append(
                [
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ozon_account,
                    posting_number,
                    err_text,
                ]
            )
            _send_telegram_error(ozon_account, posting_number, err_text)

    return errors

def sync_fbs_orders(dry_run: bool = True, limit: int = 100) -> None:
    """
    Основная функция синхронизации FBS-отправлений из Ozon в МойСклад.
    Работает сразу по двум аккаунтам (если включен второй).
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    tasks = [
        _sync_for_account("ozon1", dry_run=dry_run, limit=limit),
    ]

    if OZON2_ENABLED:
        tasks.append(_sync_for_account("ozon2", dry_run=dry_run, limit=limit))

    errors_auto, errors_trail = loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()

    # После обработки заказов — пишем CSV и отправляем ДВА файла с ошибками
    _append_order_errors_to_file(ERRORS_AUTO_FILE_PATH, errors_auto)
    _append_order_errors_to_file(ERRORS_TRAIL_FILE_PATH, errors_trail)

    if errors_auto:
        send_telegram_document(ERRORS_AUTO_FILE_PATH, caption="Ошибки Auto-MiX")
    if errors_trail:
        send_telegram_document(ERRORS_TRAIL_FILE_PATH, caption="Ошибки Trail Gear")


if __name__ == "__main__":
    print("Запуск синхронизации заказов Ozon с МойСклад...")
    sync_fbs_orders(dry_run=DRY_RUN_ORDERS, limit=300)
