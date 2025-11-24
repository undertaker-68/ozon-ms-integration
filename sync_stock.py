import os
from dotenv import load_dotenv

from ms_client import get_stock_all
from ozon_client import update_stocks, get_products_state_by_offer_ids
from notifier import send_telegram_message

load_dotenv()

WAREHOUSE_ID_ENV = os.getenv("OZON_WAREHOUSE_ID")
if not WAREHOUSE_ID_ENV:
    raise RuntimeError("Не задан OZON_WAREHOUSE_ID в .env")
WAREHOUSE_ID = int(WAREHOUSE_ID_ENV)

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"


def build_ozon_stocks_from_ms(limit: int = 50) -> list[dict]:
    """
    Берём первые `limit` строк отчёта по остаткам из МойСклад
    и превращаем их в список словарей для Ozon.

    Ожидается, что get_stock_all возвращает:
    {
        "rows": [
            {
                "article": "...",
                "stock": 10,
                ...
            },
            ...
        ]
    }
    """
    ms_data = get_stock_all(limit=limit, offset=0)
    rows = ms_data.get("rows", [])
    stocks: list[dict] = []

    for row in rows:
        article = row.get("article")
        stock_value = row.get("stock")  # доступный остаток

        if not article:
            # если у товара нет артикула - пропускаем
            continue

        try:
            stock_int = int(stock_value) if stock_value is not None else 0
        except (ValueError, TypeError):
            stock_int = 0

        if stock_int < 0:
            stock_int = 0

        stocks.append(
            {
                "offer_id": article,  # артикул = offer_id в Ozon
                "stock": stock_int,
                "warehouse_id": WAREHOUSE_ID,
            }
        )

    return stocks


def main(dry_run: bool | None = None, limit: int = 50) -> None:
    """
    dry_run:
      - True  -> только печатаем, что отправили бы в Ozon.
      - False -> реально отправляем запрос в Ozon.
      - None  -> берём значение из переменной окружения DRY_RUN.
    """
    if dry_run is None:
        dry_run = DRY_RUN

    print(f"DRY_RUN = {dry_run}")
    print(f"WAREHOUSE_ID = {WAREHOUSE_ID}")

    stocks = build_ozon_stocks_from_ms(limit=limit)
    if not stocks:
        print("Нет данных по остаткам из МойСклад.")
        return

    print(f"Сформировано {len(stocks)} позиций для обновления остатков в Ozon.")
    print("Пример первых 5 позиций:")
    for item in stocks[:5]:
        print(item)

    # --- Фильтрация по состоянию товара в Ozon ---
    offer_ids = [s["offer_id"] for s in stocks if s.get("offer_id")]
    states = get_products_state_by_offer_ids(offer_ids)

    print("\nСостояния товаров в Ozon (первые 10):")
    for oid in offer_ids[:10]:
        st = states.get(oid)
        print(f"  {oid}: {st}")

    BLOCKED_STATES = {"ARCHIVED", "DISABLED"}  # Архив / сняты с продажи

    filtered_stocks: list[dict] = []
    skipped_blocked: list[tuple[str, str | None]] = []
    skipped_unknown: list[str] = []

    for s in stocks:
        oid = s.get("offer_id")
        state = states.get(oid)

        if state in BLOCKED_STATES:
            skipped_blocked.append((oid, state))
            continue

        if state is None:
            # Ozon не знает такой offer_id (товар не заведен / удалён)
            skipped_unknown.append(oid)
            continue

        filtered_stocks.append(s)

    print(f"\nПосле фильтрации по состоянию Ozon осталось {len(filtered_stocks)} позиций.")
    if skipped_blocked:
        print("Пропущены как ARCHIVED/DISABLED:")
        for oid, st in skipped_blocked[:10]:
            print(f"  {oid}: {st}")

    if skipped_unknown:
        print("Пропущены, т.к. Ozon не знает offer_id (state=None):")
        for oid in skipped_unknown[:10]:
            print(f"  {oid}")

    stocks = filtered_stocks
    if not stocks:
        print("После фильтрации по состоянию в Ozon не осталось позиций для обновления.")
        return

    # --- Telegram: уведомления по обнулению остатка ---
    zero_offers = [s["offer_id"] for s in stocks if s.get("stock") == 0]
    if zero_offers:
        for oid in zero_offers:
            msg = (
                "ℹ️ Товар на складе Ozon закончился.\n"
                f"offer_id: {oid}\n"
                "Передаётся остаток 0 из МойСклад."
            )
            print("Telegram уведомление:", msg.replace("\n", " | "))
            try:
                send_telegram_message(msg)
            except Exception as e:
                print(f"Не удалось отправить сообщение в Telegram: {e!r}")

    if dry_run:
        print("\nРежим DRY_RUN=TRUE: данные в Ozon НЕ отправляются.")
        return

    # --- Боевой режим ---
    print("\nОтправка остатков в Ozon...")
    resp = update_stocks(stocks)
    print("Ответ Ozon:")
    print(resp)


if __name__ == "__main__":
    # Жёстко завязано на DRY_RUN из .env, чтобы случайно не включить боевой режим
    main(dry_run=None)
