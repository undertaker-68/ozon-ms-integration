import os
import csv
import tempfile
from datetime import datetime
from dotenv import load_dotenv

from ms_client import get_stock_all
from ozon_client import get_products_state_by_offer_ids, update_stocks

try:
    from notifier import send_telegram_message, send_telegram_document
except ImportError:
    def send_telegram_message(text: str) -> bool:
        print("Telegram notifier не доступен:", text)
        return False
        
    def send_telegram_document(file_path: str, caption: str = "") -> bool:
        print("Telegram notifier не доступен для файла:", file_path)
        return False

load_dotenv()

# Режим "сухого" запуска
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# Игнор-лист артикулов, которые не нужно трогать в Ozon (через запятую)
# Пример в .env:
# IGNORE_STOCK_OFFERS=14111,10561.1
IGNORE_STOCK_OFFERS = set(
    offer.strip() for offer in os.getenv("IGNORE_STOCK_OFFERS", "").split(",") if offer.strip()
)


def _parse_warehouse_map() -> dict[str, int]:
    """
    Парсит карту "склад МС → склад Ozon" из переменных окружения.

    Новый формат (рекомендуется):
      OZON_WAREHOUSE_MAP=MS_STORE_ID1:OZON_WH1,MS_STORE_ID2:OZON_WH2,...

    Для совместимости со старой схемой:
      MS_OZON_STORE_ID=<GUID склада МС>
      OZON_WAREHOUSE_ID=<ID склада Ozon>

    Если OZON_WAREHOUSE_MAP не задан, но есть MS_OZON_STORE_ID и OZON_WAREHOUSE_ID,
    будет использовано старое поведение "один склад МС → один склад Ozon".
    """
    warehouse_map: dict[str, int] = {}

    raw_map = os.getenv("OZON_WAREHOUSE_MAP", "").strip()
    if raw_map:
        for pair in raw_map.split(","):
            pair = pair.strip()
            if not pair:
                continue
            try:
                ms_store_id, ozon_wh_id = pair.split(":", 1)
                ms_store_id = ms_store_id.strip()
                ozon_wh_id_int = int(ozon_wh_id.strip())
                if ms_store_id and ozon_wh_id_int:
                    warehouse_map[ms_store_id] = ozon_wh_id_int
            except ValueError:
                print(f"[STOCK] ⚠ Некорректная пара в OZON_WAREHOUSE_MAP: {pair!r}")

    # Fallback на старые переменные
    if not warehouse_map:
        ms_store_id = os.getenv("MS_OZON_STORE_ID", "").strip()
        ozon_wh = os.getenv("OZON_WAREHOUSE_ID", "").strip()
        if ms_store_id and ozon_wh:
            try:
                warehouse_map[ms_store_id] = int(ozon_wh)
            except ValueError:
                print(f"[STOCK] ⚠ Некорректный OZON_WAREHOUSE_ID: {ozon_wh!r}")

    if not warehouse_map:
        raise RuntimeError(
            "Не настроены склады для синхронизации.\n"
            "Задай либо OZON_WAREHOUSE_MAP=MS_STORE_ID:OZON_WAREHOUSE_ID,...\n"
            "либо старую пару MS_OZON_STORE_ID + OZON_WAREHOUSE_ID в .env"
        )

    print("[STOCK] Карта складов (МС → Ozon):")
    for ms_id, wh_id in warehouse_map.items():
        print(f"  MS store {ms_id} → Ozon warehouse_id {wh_id}")

    return warehouse_map


WAREHOUSE_MAP = _parse_warehouse_map()


def _fetch_ms_stock_rows_for_store(ms_store_id: str, limit: int = 1000) -> list[dict]:
    """
    Тянем все строки отчёта /report/stock/all из МойСклад по КОНКРЕТНОМУ складу.
    """
    rows: list[dict] = []
    offset = 0

    while True:
        # ВАЖНО: get_stock_all должен уметь принимать store_id
        data = get_stock_all(limit=limit, offset=offset, store_id=ms_store_id)
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
    Собираем список остатков для отправки в Ozon по нескольким складам МС.
    Учитываем ТОЛЬКО те склады, которые описаны в WAREHOUSE_MAP.

    ВАЖНО:
      - Ozon не принимает отрицательные остатки, поэтому stock < 0 → 0.
      - Товары из IGNORE_STOCK_OFFERS полностью пропускаются.
      - Остатки передаются отдельно по каждому складу Ozon (warehouse_id из WAREHOUSE_MAP).
    """
    candidates: list[tuple[str, int, int]] = []  # (article, stock, ozon_warehouse_id)

    # 1. Собираем остатки по каждому складу МС, участвующему в интеграции
    for ms_store_id, ozon_wh_id in WAREHOUSE_MAP.items():
        print(f"[STOCK] Читаем остатки из МС: store_id={ms_store_id} → Ozon warehouse_id={ozon_wh_id}")
        rows = _fetch_ms_stock_rows_for_store(ms_store_id, limit=1000)

        for row in rows:
            article = row.get("article")
            if not article:
                continue

            if article in IGNORE_STOCK_OFFERS:
                print(f"[STOCK] ⛔ Пропуск по игнор-листу: {article}")
                continue

            stock = row.get("stock")
            try:
                stock_int = int(stock)
            except (TypeError, ValueError):
                stock_int = 0

            if stock_int < 0:
                print(f"[STOCK] В МС отрицательный остаток, принудительно ставим 0: {article} (raw={stock})")
                stock_int = 0

            candidates.append((article, stock_int, ozon_wh_id))

    if not candidates:
        print("[STOCK] Нет кандидатов для отправки в Ozon (список остатков пуст).")
        return [], 0

    # 2. Проверяем, какие offer_id вообще существуют в Ozon (по артикулу, без привязки к складу)
    offer_ids = list({art for art, _, _ in candidates})
    states = get_products_state_by_offer_ids(offer_ids)

    stocks: list[dict] = []
    skipped_not_found = 0

    for article, stock, ozon_wh_id in candidates:
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
                "warehouse_id": ozon_wh_id,
            }
        )

        # Уведомление, что в МС остаток 0 и мы его передаём в Ozon по конкретному складу
        if stock == 0:
            text = (
                "ℹ️ Товар на складе Ozon закончился.\n"
                f"offer_id: {article}\n"
                f"Склад Ozon (warehouse_id): {ozon_wh_id}\n"
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
      - формат: 'Интеграция выполнена успешно, ошибок нет' + 'Товар (склад) - количество'.
    """
    if errors_present:
        return
    if not stocks:
        return

    lines = []
    for s in stocks[:20]:
        lines.append(f"{s['offer_id']} (wh={s['warehouse_id']}) - {s['stock']}")

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

    print(f"[STOCK] DRY_RUN={dry_run}")

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
