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

    Ожидается, что get_stock_all возвращает dict с ключом "rows".
    Каждая row — dict с полями "article" и "stock".
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
                "offer_id": article,       # артикул = offer_id в Ozon
                "stock": stock_int,        # количество
                "warehouse_id": WAREHOUSE_ID,
            }
        )

    return stocks


def main(dry_run: bool | None = None, limit: int = 100) -> None:
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

    # --- Фильтрация по состоянию товара на Ozon ---

    offer_ids = [s["offer_id"] for s in stocks]
    states = get_products_state_by_offer_ids(offer_ids)

    active_stocks: list[dict] = []
    skipped_blocked: list[str] = []
    skipped_unknown: list[str] = []

    BLOCKED_STATES = {"ARCHIVED", "DISABLED"}  # Архив / сняты с продажи

    for item in stocks:
        oid = item["offer_id"]
        state = states.get(oid)

        if state is None:
            # Ozon не знает такой товар (нет в кабинете)
            skipped_unknown.append(oid)
            continue

        state_str = str(state).upper()

        if state_str in BLOCKED_STATES:
            skipped_blocked.append(f"{oid} ({state_str})")
            continue

        active_stocks.append(item)

    if skipped_blocked:
        print("Следующие товары на Ozon ARCHIVED/DISABLED, остатки НЕ отправляем:")
        for s
