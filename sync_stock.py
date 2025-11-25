import os
from dotenv import load_dotenv

from ms_client import get_stock_all
from ozon_client import get_products_state_by_offer_ids, update_stocks

try:
    from notifier import send_telegram_message
except ImportError:
    def send_telegram_message(text: str) -> bool:
        print("Telegram notifier не доступен:", text)
        return False

load_dotenv()

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
WAREHOUSE_ID = int(os.getenv("OZON_WAREHOUSE_ID", "0"))


def _fetch_ms_stock_rows(limit: int = 1000) -> list[dict]:
    """
    Тянем все строки отчёта /report/stock/all.
    """
    rows: list[dict] = []
    offset = 0

    while True:
        data = get_stock_all(limit=limit, offset=offset)
        batch = data.get("rows", [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit

    return rows


def build_ozon_stocks_from_ms() -> tuple[list[dict], int]:
    """
    Собираем список остатков для отправки в Ozon + считаем,
    сколько товаров было пропущено, потому что Ozon их не знает.

    ВАЖНО: Ozon не принимает отрицательные остатки,
    поэтому все stock < 0 принудительно превращаем в 0.
    """
    rows = _fetch_ms_stock_rows(limit=1000)

    candidates: list[tuple[str, int]] = []
    for row in rows:
        article = row.get("article")
        if not article:
            continue
        stock = row.get("stock")
        try:
            stock_int = int(stock)
        except (TypeError, ValueError):
            stock_int = 0

        if stock_int < 0:
            print(f"[STOCK] В МС отрицательный остаток, принудительно ставим 0: {article} (raw={stock})")
            stock_int = 0

        candidates.append((article, stock_int))

    offer_ids = [art for art, _ in candidates]
    states = get_products_state_by_offer_ids(offer_ids)

    stocks: list[dict] = []
    skipped_not_found = 0

    for article, stock in candidates:
        state = states.get(article)

        # Ozon вообще не знает такой offer_id
        if state is None:
            skipped_not_found += 1
            continue

        # ARCHIVED / autoarchived не трогаем
        if state != "ACTIVE":
            continue

        stocks.append(
            {
                "offer_id": article,
                "stock": stock,
                "warehouse_id": WAREHOUSE_ID,
            }
        )

        # Уведомление, что в МС остаток 0 и мы его передаём в Ozon
        if stock == 0:
            text = (
                "ℹ️ Товар на складе Ozon закончился.\n"
                f"offer_id: {article}\n"
                f"Передаётся остаток 0 из МойСклад."
            )
            print("[STOCK]", text.replace("\n", " | "))
            try:
                send_telegram_message(text)
            except Exception:
                pass

    return stocks, skipped_not_found


def _send_success_summary_telegram(stocks: list[dict], errors_present: bool) -> None:
    """
    Отправляем краткий итог по остаткам:
      - только если ошибок нет;
      - формат: 'Интеграция выполнена успешно, ошибок нет' + 'Товар - количество'.
    """
    if errors_present:
        return
    if not stocks:
        return

    lines = []
    for s in stocks[:20]:
        lines.append(f"{s['offer_id']} - {s['stock']}")

    if len(stocks) > 20:
        lines.append(f"... и ещё {len(stocks) - 20} позиций")

    text = "Интеграция выполнена успешно, ошибок нет.\nОбновлены остатки:\n" + "\n".join(lines)

    try:
        send_telegram_message(text)
    except Exception as e:
        print(f"[STOCK] Не удалось отправить Telegram-резюме: {e!r}")


def main(dry_run: bool | None = None) -> None:
    if dry_run is None:
        dry_run = DRY_RUN

    print(f"[STOCK] DRY_RUN={dry_run}, WAREHOUSE_ID={WAREHOUSE_ID}")

    stocks, skipped_not_found = build_ozon_stocks_from_ms()
    print(f"[STOCK] Пропущено (товар не найден на Ozon): {skipped_not_found}")
    print(f"[STOCK] Позиций для отправки в Ozon: {len(stocks)}")

    if dry_run:
        print("[STOCK] DRY_RUN=TRUE: запрос к Ozon не отправляется.")
        return

    if not stocks:
        print("[STOCK] Список остатков пуст, обновлять в Ozon нечего.")
        return

    print("[STOCK] Отправляем остатки в Ozon...")

    data = update_stocks(stocks)
    result_items = data.get("result", []) if isinstance(data, dict) else []
    errors_present = any((item.get("errors") or []) for item in result_items)

    if not errors_present:
        updated_count = len(result_items) if result_items else len(stocks)
        print(f"[STOCK] Обновлено в Ozon: {updated_count} позиций")
    else:
        print("[STOCK] Обновление в Ozon завершено с ошибками (подробности в логах / Telegram).")

    # Итоговое уведомление в Telegram: только если ошибок нет
    _send_success_summary_telegram(stocks, errors_present)


if __name__ == "__main__":
    main(dry_run=None)
