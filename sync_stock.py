import os
from dotenv import load_dotenv

from ms_client import get_stock_all
from ozon_client import update_stocks, get_products_state_by_offer_ids
from notifier import send_telegram_message

load_dotenv()

WAREHOUSE_ID = int(os.getenv("OZON_WAREHOUSE_ID", "0"))
if not WAREHOUSE_ID:
    raise RuntimeError("Не задан OZON_WAREHOUSE_ID в .env")

# DRY-RUN для остатков
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"


def build_ozon_stocks_from_ms(limit: int = 100) -> list[dict]:
    """
    Забираем остатки из МойСклад и формируем список позиций
    для обновления в Ozon.

    Формат каждой позиции:
    {
        "offer_id": <артикул_из_МС>,
        "stock": <доступно>,
        "warehouse_id": WAREHOUSE_ID,
    }
    """
    ms_items = get_stock_all(limit=limit)

    stocks: list[dict] = []

    for item in ms_items:
        article = item.get("article") or item.get("name")
        if not article:
            # артикул пустой — пропускаем
            continue

        stock_value = item.get("stock")  # поле "Доступно" из МойСклад

        try:
            stock_int = int(stock_value) if stock_value is not None else 0
        except (TypeError, ValueError):
            stock_int = 0

        if stock_int < 0:
            stock_int = 0

        stocks.append(
            {
                "offer_id": article,  # артикул = offer_id в Ozon
                "stock": stock_int,   # количество
                "warehouse_id": WAREHOUSE_ID,
            }
        )

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

    print(f"DRY_RUN = {dry_run}")
    print(f"WAREHOUSE_ID = {WAREHOUSE_ID}")

    stocks = build_ozon_stocks_from_ms(limit=1000)

    if not stocks:
        print("Нет данных по остаткам из МойСклад.")
        return

    # --- Фильтрация по состоянию товара на Ozon ---
    # Берём только те товары, которые на Ozon "В продаже", "Готовы к продаже",
    # "Ошибки", "На доработку". Сняты с продажи / Архив — игнорируем.

    offer_ids = list({item["offer_id"] for item in stocks})
    states = get_products_state_by_offer_ids(offer_ids)

    active_stocks: list[dict] = []
    skipped_archived: list[str] = []
    skipped_no_state: list[str] = []

    for item in stocks:
        oid = item["offer_id"]
        state = states.get(oid)

        if state is None:
            # на всякий случай тоже учитываем, но помечаем
            active_stocks.append(item)
            skipped_no_state.append(oid)
            continue

        state_str = str(state).upper()

        # Логика:
        # - ARCHIVED, DISABLED, SUSPENDED и т.п. — пропускаем
        # - всё остальное (ACTIVE, ERROR, READY_TO_ACTIVATION и т.д.) — берём
        if "ARCHIVE" in state_str or "ARCHIVED" in state_str or "DISABLED" in state_str:
            skipped_archived.append(f"{oid} ({state_str})")
            continue

        # Оставляем как активный
        active_stocks.append(item)

    if skipped_archived:
        print("Следующие товары на Ozon в архиве/сняты, остатки НЕ отправляем:")
        for s in skipped_archived:
            print("  -", s)

    if skipped_no_state:
        print("Для части товаров Ozon не вернул state, они будут обновлены как есть.")
        print("Примеры offer_id:", ", ".join(skipped_no_state[:10]))

    stocks = active_stocks

    if not stocks:
        print("После фильтрации по состояниям Ozon не осталось позиций для обновления.")
        return

    print(f"Сформировано {len(stocks)} позиций для обновления остатков в Ozon.")
    print("Пример первых 5 позиций:")
    for item in stocks[:5]:
        print(item)

    # --- Телеграм уведомление: «товар Ozon не найден в МойСклад» ---
    #
    # Это уже реализовано в sync_orders.py для заказов.
    # Для остатков ситуация другая: мы строим список ИЗ МойСклад, поэтому
    # здесь "Ozon не найден в МС" не возникает. Этот кейс закрыт в sync_orders.py.

    if dry_run:
        print("\nРежим DRY_RUN=TRUE: данные в Ozon НЕ отправляются.")
        return

    # Если dry_run=False -> реально отправляем
    print("\nОтправка остатков в Ozon...")
    resp = update_stocks(stocks)
    print("Ответ Ozon:")
    print(resp)

    # --- Телеграм-уведомление: товар на складе Ozon «закончился» ---
    # Для простоты: отправляем уведомление для тех позиций, где передали stock == 0.
    zero_items = [s for s in stocks if s.get("stock", 0) == 0]

    if zero_items:
        lines = ["⚠ В МойСклад передан остаток 0, считаем, что товар на Ozon закончился:"]
        for item in zero_items[:50]:  # ограничимся 50 строками
            lines.append(
                f"- offer_id: {item['offer_id']}, warehouse_id: {item['warehouse_id']}, stock: 0"
            )
        msg = "\n".join(lines)
        print(msg)
        send_telegram_message(msg)


if __name__ == "__main__":
    # Жёстко завязано на DRY_RUN из .env, чтобы случайно не включить боевой режим
    main(dry_run=None)
