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
    get_stock_by_assortment_href,
)

try:
    from notifier import send_telegram_message
except ImportError:
    def send_telegram_message(text: str) -> bool:
        print("Telegram notifier не доступен:", text)
        return False


load_dotenv()

DRY_RUN_ORDERS = os.getenv("DRY_RUN_ORDERS", "true").lower() == "true"

# Статусы заказа в МС (meta.href)
MS_STATE_AWAIT_PACK = os.getenv("MS_STATE_AWAIT_PACK")      # Ожидают сборки
MS_STATE_AWAIT_SHIP = os.getenv("MS_STATE_AWAIT_SHIP")      # Ожидают отгрузки
MS_STATE_DELIVERING = os.getenv("MS_STATE_DELIVERING")      # Доставляются
MS_STATE_DELIVERED = os.getenv("MS_STATE_DELIVERED")        # Доставлен
MS_STATE_CANCELLED = os.getenv("MS_STATE_CANCELLED")        # Отменён/закрыт

# Организация, контрагент и склад — через .env
MS_ORGANIZATION_HREF = os.getenv("MS_ORGANIZATION_HREF")
MS_AGENT_HREF = os.getenv("MS_AGENT_HREF")
MS_STORE_HREF = os.getenv("MS_STORE_HREF")

if not (MS_ORGANIZATION_HREF and MS_AGENT_HREF and MS_STORE_HREF):
    raise RuntimeError(
        "Не заданы MS_ORGANIZATION_HREF / MS_AGENT_HREF / MS_STORE_HREF в .env. "
        "Скопируй их из meta.href существующего заказа/организации/контрагента/склада в МойСклад."
    )


def build_ms_positions_from_posting(posting: dict) -> list[dict]:
    """
    Для одного отправления Ozon строим список позиций МС:
    [{'quantity': X, 'ms_meta': {...}, 'article': '...'}, ...]
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
                "article": offer_id,
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
    """
    Формируем заказ покупателя для МойСклад.
    ИМЯ ЗАКАЗА = номер отправления Ozon (БЕЗ префикса OZON-).
    description: 'FBS → Auto-Mix'
    """
    posting_number = posting.get("posting_number", "NO_NUMBER")
    order_name = posting_number

    payload = {
        "name": order_name,
        "description": "FBS \u2192 Auto-Mix",
        "organization": {
            "meta": {
                "href": MS_ORGANIZATION_HREF,
                "type": "organization",
                "mediaType": "application/json",
            }
        },
        "agent": {
            "meta": {
                "href": MS_AGENT_HREF,
                "type": "counterparty",
                "mediaType": "application/json",
            }
        },
        "store": {
            "meta": {
                "href": MS_STORE_HREF,
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


def notify_zero_stock_if_changed(
    posting: dict,
    ms_positions: list,
    stocks_before: dict[str, int | None],
) -> None:
    """
    После обработки отправления (обычно статус delivering) проверяем:
    если у какого-то артикула остаток
