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


def notify_zero_stock_if_changed(posting: dict, ms_positions: list, stocks_before: dict[str, int | None]) -> None:
    """
    После обработки отправления (обычно статус delivering) проверяем:
    если у какого-то артикула остаток в МС был >0, а стал 0 — шлём уведомление.
    Остатки считаем по тому же складу MS_OZON_STORE_HREF, фильтрация по assortment.
    """
    posting_number = posting.get("posting_number")
    changed = []

    for pos in ms_positions:
        article = pos.get("article")
        if not article:
            continue

        ms_meta = pos.get("ms_meta") or {}
        assortment_meta = ms_meta.get("href")
        if not assortment_meta:
            continue

        before = stocks_before.get(article)

        after = get_stock_by_assortment_href(assortment_meta)
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

    text = "ℹ️ В МойСклад остаток товара стал 0 после обработки заказа из Ozon.\n" + "\n".join(lines)

    print("[ORDERS]", text.replace("\n", " | "))
    try:
        send_telegram_message(text)
    except Exception as e:
        print(f"[ORDERS] Не удалось отправить Telegram: {e!r}")


def _find_existing_order_by_posting_number(posting_number: str) -> dict | None:
    """
    Для плавного перехода:
      - сначала ищем заказ с именем = posting_number
      - если нет, ищем старый вариант: OZON-{posting_number}
    """
    if not posting_number:
        return None

    name_new = posting_number
    name_old = f"OZON-{posting_number}"

    order = find_customer_order_by_name(name_new)
    if order:
        return order

    return find_customer_order_by_name(name_old)


def process_posting(posting: dict, dry_run: bool) -> None:
    posting_number = posting.get("posting_number")
    status = posting.get("status")
    print(f"[ORDERS] Обработка отправления {posting_number}, статус Ozon: {status}")

    ms_positions = build_ms_positions_from_posting(posting)
    if not ms_positions:
        print(f"[ORDERS] Пропускаем {posting_number}: нет ни одной позиции в МС")
        return

    order_name = posting_number
    existing_order = _find_existing_order_by_posting_number(posting_number)

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
            print(f"[ORDERS] {order_name}: заказ не найден в МС, создаём.")
            if dry_run:
                print("[ORDERS] DRY_RUN_ORDERS=TRUE: создание заказа пропущено.")
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
    # и проверить, не ушёл ли остаток в 0

    stocks_before: dict[str, int | None] = {}

    # Снимаем остатки "до" по каждому ассортимента по складу Ozon
    for pos in ms_positions:
        article = pos.get("article")
        ms_meta = pos.get("ms_meta") or {}
        assortment_meta = ms_meta.get("href")
        if not article or not assortment_meta:
            continue
        stocks_before[article] = get_stock_by_assortment_href(assortment_meta)

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

    notify_zero_stock_if_changed(posting, ms_positions, stocks_before)
    print(f"[ORDERS] Заказ {order_name}: резерв снят, отгрузка создана.")

    elif status == "cancelled":
        # Отмена: снять резерв и поставить статус "Отменён"
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
