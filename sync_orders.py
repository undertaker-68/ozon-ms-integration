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

MS_STATE_AWAIT_PACK = os.getenv("MS_STATE_AWAIT_PACK")
MS_STATE_AWAIT_SHIP = os.getenv("MS_STATE_AWAIT_SHIP")
MS_STATE_DELIVERING = os.getenv("MS_STATE_DELIVERING")
MS_STATE_DELIVERED = os.getenv("MS_STATE_DELIVERED")
MS_STATE_CANCELLED = os.getenv("MS_STATE_CANCELLED")

MS_ORGANIZATION_HREF = os.getenv("MS_ORGANIZATION_HREF")
MS_AGENT_HREF = os.getenv("MS_AGENT_HREF")
MS_STORE_HREF = os.getenv("MS_STORE_HREF")


if not (MS_ORGANIZATION_HREF and MS_AGENT_HREF and MS_STORE_HREF):
    raise RuntimeError(
        "Не заданы MS_ORGANIZATION_HREF / MS_AGENT_HREF / MS_STORE_HREF. "
        "Скопируйте meta.href из МойСклад."
    )


def build_ms_positions_from_posting(posting: dict) -> list[dict]:
    """
    Собираем позиции МС из одного отправления Ozon.
    Если хотя бы один offer_id не найден — возвращаем пустой список.
    """
    products = posting.get("products") or []
    ms_positions = []
    missing = []

    for p in products:
        offer_id = p.get("offer_id")
        qty = p.get("quantity", 0)
        if not offer_id or qty <= 0:
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
            "❗ Не найдены товары в МойСклад по артикулу:\n"
            f"Отправление: {posting.get('posting_number')}\n"
            f"Артикулы: {', '.join(missing)}"
        )
        print("[ORDERS]", text.replace("\n", " | "))
        try:
            send_telegram_message(text)
        except Exception:
            pass
        return []

    return
