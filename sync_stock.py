import os
from dotenv import load_dotenv

from ms_client import get_stock_all
from ozon_client import update_stocks

load_dotenv()

WAREHOUSE_ID = os.getenv("OZON_WAREHOUSE_ID")

if not WAREHOUSE_ID:
    raise RuntimeError("Не задан OZON_WAREHOUSE_ID в .env")

WAREHOUSE_ID = int(WAREHOUSE_ID)


def build_ozon_stocks_from_ms(limit: int = 100) -> list:
    """
    Берём первые `limit` строк отчёта по остаткам из МойСклад
    и превращаем их в список словарей для Ozon.
    """
    ms_data = get_stock_all(limit=limit, offset=0)
    rows = ms_data.get("rows", [])

    stocks = []
    for row in rows:
        article = row.get("article")
        stock_value = row.get("stock")  # доступный остаток

        if not article:
            # если у товара нет артикула - пропускаем
            continue

        try:
            stock_int = int(stock_value) if stock_value is not None else 0
        except ValueError:
            stock_int = 0

        if stock_int < 0:
            stock_int = 0

        stocks.append({
            "offer_id": article,      # артикул = offer_id в Ozon
            "stock": stock_int,       # количество
            "warehouse_id": WAREHOUSE_ID,
        })

    return stocks


def main(dry_run: bool = True):
    """
    dry_run=True  -> только печатаем, что отправили бы в Ozon.
    dry_run=False -> реально отправляем запрос в Ozon.
    """
    stocks = build_ozon_stocks_from_ms(limit=50)  # пока ограничимся 50 строками

    if not stocks:
        print("Нет данных по остаткам из МойСклад.")
        return

    print(f"Сформировано {len(stocks)} позиций для обновления остатков в Ozon.")
    print("Пример 5 позиций:")
    for item in stocks[:5]:
        print(item)

    if dry_run:
        print("\nРежим dry_run=TRUE: данные в Ozon НЕ отправляются.")
        return

    # Если dry_run=False -> реально отправляем
    resp = update_stocks(stocks)
    print("\nОтвет Ozon на обновление остатков:")
    print(resp)


if __name__ == "__main__":
    # ПЕРВЫЙ ЗАПУСК: оставляем dry_run=True, чтобы ничего не менять на Ozon
    main(dry_run=True)

