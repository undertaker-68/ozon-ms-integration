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

    return ms_positions


def build_customer_order_payload(posting: dict, ms_positions: list) -> dict:
    """
    Формируем payload для создания заказа в МойСклад.
    """
    posting_number = posting.get("posting_number", "NO_NUMBER")

    payload = {
        "name": posting_number,
        "description": "FBS → Auto-Mix",
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


def notify_zero_stock_if_changed(posting: dict, ms_positions: list, stocks_before: dict[str, int | None]):
    """
    Проверяем: не стало ли количество на складе 0 после списания.
    """
    posting_number = posting.get("posting_number")
    changed = []

    for pos in ms_positions:
        article = pos["article"]
        assortment_href = pos["ms_meta"]["href"]

        before = stocks_before.get(article)
        after = get_stock_by_assortment_href(assortment_href)

        try:
            b = int(before) if before is not None else None
            a = int(after) if after is not None else None
        except (TypeError, ValueError):
            continue

        if b is not None and b > 0 and a == 0:
            changed.append((article, b, a))

    if not changed:
        return

    lines = [f"Отправление: {posting_number}"]
    for article, b, a in changed:
        lines.append(f"Артикул: {article} | было: {b} | стало: {a}")

    text = "ℹ️ Остаток стал 0 после обработки заказа:\n" + "\n".join(lines)
    print("[ORDERS]", text.replace("\n", " | "))

    try:
        send_telegram_message(text)
    except Exception:
        pass


def _find_existing_order_by_posting(posting_number: str) -> dict | None:
    """
    Ищем заказ:
    1) по имени = posting_number
    2) по старому формату OZON-<номер>
    """
    if not posting_number:
        return None

    order = find_customer_order_by_name(posting_number)
    if order:
        return order

    return find_customer_order_by_name(f"OZON-{posting_number}")


def process_posting(posting: dict, dry_run: bool):
    posting_number = posting.get("posting_number")
    status = posting.get("status")

    print(f"[ORDERS] Обработка {posting_number}, статус: {status}")

    ms_positions = build_ms_positions_from_posting(posting)
    if not ms_positions:
        return

    existing = _find_existing_order_by_posting(posting_number)

    # ================================
    # awaiting_packaging → создать заказ
    # ================================
    if status == "awaiting_packaging":
        if existing:
            print(f"[ORDERS] Заказ {posting_number} уже существует.")
            return

        print(f"[ORDERS] Создание заказа {posting_number}")
        if dry_run:
            print("[ORDERS] DRY_RUN=True — заказ не создаётся.")
            return

        payload = build_customer_order_payload(posting, ms_positions)
        created = create_customer_order(payload)
        if MS_STATE_AWAIT_PACK:
            update_customer_order_state(created["meta"]["href"], MS_STATE_AWAIT_PACK)
        print(f"[ORDERS] Заказ {posting_number} создан.")

        return

    # ================================
    # awaiting_deliver → статус "Ожидают отгрузки"
    # ================================
    if status == "awaiting_deliver":
        if not existing:
            print(f"[ORDERS] Заказ {posting_number} не найден — создаём.")
            if dry_run:
                print("[ORDERS] DRY_RUN=True — пропуск создания.")
                return
            payload = build_customer_order_payload(posting, ms_positions)
            existing = create_customer_order(payload)

        print(f"[ORDERS] Перевод {posting_number} в 'Ожидают отгрузки'")

        if dry_run:
            print("[ORDERS] DRY_RUN=True — статус не меняем.")
            return

        if MS_STATE_AWAIT_SHIP:
            update_customer_order_state(existing["meta"]["href"], MS_STATE_AWAIT_SHIP)

        print(f"[ORDERS] {posting_number}: статус обновлён.")
        return

    # ================================
    # delivering → списание + отгрузка
    # ================================
    if status == "delivering":
        stocks_before = {}
        for pos in ms_positions:
            article = pos["article"]
            href = pos["ms_meta"]["href"]
            stocks_before[article] = get_stock_by_assortment_href(href)

        if not existing:
            print(f"[ORDERS] {posting_number} отсутствует — создаём.")
            if dry_run:
                print("[ORDERS] DRY_RUN=True — пропуск создания.")
                return
            payload = build_customer_order_payload(posting, ms_positions)
            existing = create_customer_order(payload)

        href = existing["meta"]["href"]

        print(f"[ORDERS] {posting_number}: статус 'Доставляются', снятие резерва и отгрузка")

        if dry_run:
            print("[ORDERS] DRY_RUN=True — действия не выполняются.")
            return

        if MS_STATE_DELIVERING:
            update_customer_order_state(href, MS_STATE_DELIVERING)

        clear_reserve_for_order(href)
        create_demand_from_order(href)

        notify_zero_stock_if_changed(posting, ms_positions, stocks_before)

        print(f"[ORDERS] {posting_number}: отгрузка создана.")
        return

    # ================================
    # cancelled → отмена
    # ================================
    if status == "cancelled":
        if not existing:
            print(f"[ORDERS] {posting_number}: заказ отсутствует.")
            return

        href = existing["meta"]["href"]
        print(f"[ORDERS] {posting_number}: отмена")

        if dry_run:
            print("[ORDERS] DRY_RUN=True — отмена пропущена.")
            return

        clear_reserve_for_order(href)
        if MS_STATE_CANCELLED:
            update_customer_order_state(href, MS_STATE_CANCELLED)

        print(f"[ORDERS] {posting_number}: отменён.")
        return

    # ================================
    # delivered → доставлен
    # ================================
    if status == "delivered":
        if not existing:
            print(f"[ORDERS] {posting_number}: заказа нет.")
            return

        href = existing["meta"]["href"]
        print(f"[ORDERS] {posting_number}: доставка завершена")

        if dry_run:
            print("[ORDERS] DRY_RUN=True — статус не меняем.")
            return

        clear_reserve_for_order(href)
        if MS_STATE_DELIVERED:
            update_customer_order_state(href, MS_STATE_DELIVERED)

        print(f"[ORDERS] {posting_number}: статус обновлён.")
        return

    print(f"[ORDERS] Статус '{status}' не обрабатывается.")


def sync_fbs_orders(dry_run: bool, limit: int = 3):
    print(f"[ORDERS] Старт sync_fbs_orders, DRY_RUN_ORDERS={dry_run}")

    data = get_fbs_postings(limit=limit)
    postings = data.get("result", {}).get("postings", [])

    print(f"[ORDERS] Найдено отправлений: {len(postings)}")

    for posting in postings:
        try:
            process_posting(posting, dry_run)
        except Exception as e:
            msg = (
                "❗ Ошибка обработки отправления Ozon\n"
                f"posting_number: {posting.get('posting_number')}\n"
                f"error: {e!r}"
            )
            print("[ORDERS]", msg.replace("\n", " | "))
            try:
                send_telegram_message(msg)
            except Exception:
                pass


if __name__ == "__main__":
    sync_fbs_orders(dry_run=DRY_RUN_ORDERS, limit=3)
