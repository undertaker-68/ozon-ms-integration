import os
from dotenv import load_dotenv

from ms_client import get_stock_all
from ozon_client import update_stocks, get_products_state_by_offer_ids
from notifier import send_telegram_message

load_dotenv()

# ID склада Ozon из .env
WAREHOUSE_ID_RAW = os.getenv("OZON_WAREHOUSE_ID", "0")

# Общий флаг dry-run для остатков (читается из .env: DRY_RUN=true/false)
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

try:
    WAREHOUSE_ID = int(WAREHOUSE_ID_RAW)
except ValueError:
    raise RuntimeError("OZON_WAREHOUSE_ID в .env должен быть числом")


def build_ozon_stocks_from_ms(limit: int = 100) -> list:
    """
    Берём первые `limit` строк отчёта по остаткам из МойСклад
    и превращаем их в список словарей для Ozon.

    Формат элемента:
    {
        "offer_id": <артикул товара = offer_id в Ozon>,
        "stock": <доступный остаток>,
        "warehouse_id": <ID склада Ozon>
    }
    """
    ms_data = get_stock_all(limit=limit, offset=0)
    rows = ms_data.get("rows", [])

    stocks = []
    for row in rows:
        article = row.get("article")
        stock_value = row.get("stock")  # доступный остаток по МойСклад

        if not article:
            # если у товара нет артикула - пропускаем (связать с Ozon не сможем)
            continue

        try:
            stock_int = int(stock_value) if stock_value is not None else 0
        except (TypeError, ValueError):
            stock_int = 0

        if stock_int < 0:
            stock_int = 0

        stocks.append({
            "offer_id": article,      # артикул = offer_id в Ozon
            "stock": stock_int,       # количество
            "warehouse_id": WAREHOUSE_ID,
        })

    return stocks


def main(dry_run: bool | None = None):
    """
    dry_run:
      - True  -> только печатаем, что отправили бы в Ozon.
      - False -> реально отправляем запрос в Ozon.
      - None  -> берём значение из переменной окружения DRY_RUN.
    """
    if dry_run is None:
        dry_run = DRY_RUN

    

    stocks = build_ozon_stocks_from_ms(limit=50)  # пока ограничимся 50 строками

    if not stocks:
        print("Нет данных по остаткам из МойСклад.")
        return

    print(f"Сформировано {len(stocks)} позиций для обновления остатков в Ozon.")
    print("Пример первых 5 позиций:")
    for item in stocks[:5]:
        print(item)

    if dry_run:
        print("\nРежим DRY_RUN=TRUE: данные в Ozon НЕ отправляются.")
        return

    # Если dry_run=False -> реально отправляем
    print("\nОтправка остатков в Ozon...")
    resp = update_stocks(stocks)
    print("Ответ Ozon:")
    print(resp)


if __name__ == "__main__":
    # Жёстко завязано на DRY_RUN из .env, чтобы случайно не включить боевой режим
    main(dry_run=None)
