import os
from dotenv import load_dotenv

from ms_client import get_stock_all
from ozon_client import update_stocks, get_products_state_by_offer_ids
from notifier import send_telegram_message

load_dotenv()

WAREHOUSE_ID_RAW = os.getenv("OZON_WAREHOUSE_ID")
if not WAREHOUSE_ID_RAW:
    raise RuntimeError("Не задан OZON_WAREHOUSE_ID в .env")

try:
    WAREHOUSE_ID = int(WAREHOUSE_ID_RAW)
except ValueError:
    raise RuntimeError("OZON_WAREHOUSE_ID в .env должен быть числом")

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"


def build_ozon_stocks_from_ms(limit: int = 100) -> list[dict]:
    """
    Берём первые `limit` строк отчёта по остаткам из МойСклад
    и превращаем их в список словарей для Ozon.
    """
    ms_data = get_stock_all(limit=limit, offset=0)
    rows = ms_data.get("rows", [])

    stocks: list[dict] = []

    for row in rows:
        article = row.get("article")
        stock_value = row.get("stock")

        if not article:
            continue

        try:
            stock_int = int(stock_value) if stock_value is not None else 0
        except (ValueError, TypeError):
            stock_int = 0

        if stock_int < 0:
            stock_int = 0

        stocks.append(
            {
                "offer_id": article,
                "stock": stock_int,
                "warehouse_id": WAREHOUSE_ID,
            }
        )

    return stocks


def main(dry_run: bool | None = None, limit: int = 100) -> None:
    if dry_run is None:
        dry_run = DRY_RUN

    print(f"[STOCK] DRY_RUN={dry_run}, WAREHOUSE_ID={WAREHOUSE_ID}")

    stocks = build_ozon_stocks_from_ms(limit=limit)

    if not stocks:
        print("[STOCK] Нет данных по остаткам из МойСклад.")
        return

    offer_ids = [s["offer_id"] for s in stocks]
    states = get_products_state_by_offer_ids(offer_ids)

    active_stocks: list[dict] = []
    skipped_blocked: list[str] = []
    skipped_unknown: list[str] = []

    BLOCKED_STATES = {"ARCHIVED", "DISABLED"}

    for item in stocks:
        oid = item["offer_id"]
        state = states.get(oid)

        if state is None:
            skipped_unknown.append(oid)
            continue

        state_str = str(state).upper()
        if state_str in BLOCKED_STATES:
            skipped_blocked.append(oid)
            continue

        active_stocks.append(item)

    if skipped_blocked:
        print(f"[STOCK] Пропущено (ARCHIVED/DISABLED на Ozon): {len(skipped_blocked)}")

    if skipped_unknown:
        print(f"[STOCK] Пропущено (товар не найден на Ozon): {len(skipped_unknown)}")

    stocks = active_stocks

    if not stocks:
        print("[STOCK] После фильтрации по состояниям Ozon позиций не осталось.")
        return

    print(f"[STOCK] Позиций для отправки в Ozon: {len(stocks)}")

    zero_items = [s for s in stocks if s.get("stock") == 0]
    for item in zero_items:
        msg = (
            "ℹ️ Товар на складе Ozon закончился.\n"
            f"offer_id: {item['offer_id']}\n"
            "Передаётся остаток 0 из МойСклад."
        )
        try:
            send_telegram_message(msg)
        except Exception as e:
            print(f"[STOCK] Не удалось отправить Telegram: {e!r}")

    if dry_run:
        print("[STOCK] DRY_RUN=TRUE: данные в Ozon не отправляются.")
        return

    print("[STOCK] Отправляем остатки в Ozon...")
    resp = update_stocks(stocks)
    updated = sum(1 for r in resp.get("result", []) if r.get("updated"))
    print(f"[STOCK] Обновлено в Ozon: {updated} позиций")


if __name__ == "__main__":
    main(dry_run=None, limit=100)
