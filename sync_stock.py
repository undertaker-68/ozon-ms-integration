import os
import csv
import tempfile
from dotenv import load_dotenv

from ms_client import get_stock_all
from ozon_client import get_products_state_by_offer_ids, update_stocks
from ozon_client2 import update_stocks as update_stocks_ozon2

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

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

IGNORE_STOCK_OFFERS = set(
    offer.strip() for offer in os.getenv("IGNORE_STOCK_OFFERS", "").split(",") if offer.strip()
)


def _parse_warehouse_map() -> dict[str, int]:
    warehouse_map: dict[str, int] = {}

    raw_map = os.getenv("OZON_WAREHOUSE_MAP", "").strip()
    if raw_map:
        for pair in raw_map.split(","):
            pair = pair.strip()
            if not pair:
                continue
            try:
                ms_store_id, ozon_wh_id = pair.split(":", 1)
                warehouse_map[ms_store_id.strip()] = int(ozon_wh_id.strip())
            except Exception:
                print(f"[WARN] Неверный формат пары: {pair}")

    if not warehouse_map:
        ms_old = os.getenv("MS_OZON_STORE_ID")
        wh_old = os.getenv("OZON_WAREHOUSE_ID")
        if ms_old and wh_old:
            warehouse_map[ms_old] = int(wh_old)

    if not warehouse_map:
        raise RuntimeError("Не заданы склады. Укажи OZON_WAREHOUSE_MAP в .env")

    print("[STOCK] Карта складов:")
    for ms_id, wh_id in warehouse_map.items():
        print(f"  MS store {ms_id} → Ozon warehouse_id {wh_id}")

    return warehouse_map


WAREHOUSE_MAP = _parse_warehouse_map()


def _fetch_ms_stock_rows_for_store(ms_store_id: str, limit: int = 1000) -> list[dict]:
    rows: list[dict] = []
    offset = 0

    while True:
        data = get_stock_all(limit=limit, offset=offset, store_id=ms_store_id)
        batch = data.get("rows", [])

        if not batch:
            break

        rows.extend(batch)

        if len(batch) < limit:
            break

        offset += limit

    return rows


def build_ozon_stocks_from_ms() -> tuple[list[dict], int, list[dict]]:
    candidates: list[tuple[str, int, int]] = []
    names_by_article: dict[str, str] = {}

    for ms_store_id, ozon_wh_id in WAREHOUSE_MAP.items():
        print(f"[STOCK] Читаем остатки из МС: store_id={ms_store_id} → Ozon warehouse_id={ozon_wh_id}")

        rows = _fetch_ms_stock_rows_for_store(ms_store_id)

        for row in rows:
            article = row.get("article")
            if not article:
                continue

            name = (
                row.get("name")
                or (row.get("assortment") or {}).get("name")
                or ""
            )

            if article in IGNORE_STOCK_OFFERS:
                continue

            stock_raw = row.get("stock", 0)
            try:
                stock_int = int(stock_raw)
            except Exception:
                stock_int = 0

            if stock_int < 0:
                stock_int = 0

            candidates.append((article, stock_int, ozon_wh_id))

            if article not in names_by_article and name:
                names_by_article[article] = name

    if not candidates:
        return [], 0, []

    offer_ids = list({art for art, _, _ in candidates})
    states = get_products_state_by_offer_ids(offer_ids)

    stocks: list[dict] = []
    skipped_not_found = 0

    for article, stock, ozon_wh_id in candidates:
        state = states.get(article)

        if state is None:
            skipped_not_found += 1
            continue

        if state != "ACTIVE":
            continue

        stocks.append({
            "offer_id": article,
            "stock": stock,
            "warehouse_id": ozon_wh_id,
        })

    report_rows = [
        {
            "name": names_by_article.get(s["offer_id"], ""),
            "article": s["offer_id"],
            "stock": s["stock"],
        }
        for s in stocks
    ]

    return stocks, skipped_not_found, report_rows

def _send_stock_report_file(report_rows: list[dict]) -> None:
    """
    Отправляем ДВА CSV-файла в Telegram:
      - Остатки Auto-MiX
      - Остатки Trail Gear
    Содержимое одинаковое, подписи разные.
    """
    if not report_rows:
        print("[STOCK] Нет данных — CSV не создан.")
        return

    fd1, path_auto = tempfile.mkstemp(prefix="ozon_stock_auto_", suffix=".csv")
    os.close(fd1)
    fd2, path_trail = tempfile.mkstemp(prefix="ozon_stock_trail_", suffix=".csv")
    os.close(fd2)

    try:
        # Заполняем оба файла одинаковыми данными
        for path in (path_auto, path_trail):
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f, delimiter=";")
                writer.writerow(["№", "Наименование", "Артикул", "Кол-во"])

                for idx, row in enumerate(report_rows, start=1):
                    writer.writerow(
                        [
                            idx,
                            row["name"],
                            row["article"],
                            row["stock"],
                        ]
                    )

        ok_auto = send_telegram_document(path_auto, caption="Остатки Auto-MiX")
        ok_trail = send_telegram_document(path_trail, caption="Остатки Trail Gear")

        if ok_auto:
            print(f"[STOCK] CSV (Auto-MiX) отправлен: {path_auto}")
        else:
            print(f"[STOCK] Ошибка отправки CSV (Auto-MiX): {path_auto}")

        if ok_trail:
            print(f"[STOCK] CSV (Trail Gear) отправлен: {path_trail}")
        else:
            print(f"[STOCK] Ошибка отправки CSV (Trail Gear): {path_trail}")

    finally:
        for path in (path_auto, path_trail):
            try:
                os.remove(path)
            except:
                pass

def main(dry_run: bool | None = None) -> None:
    if dry_run is None:
        dry_run = DRY_RUN

    print(f"[STOCK] DRY_RUN={dry_run}")

    stocks, skipped, report_rows = build_ozon_stocks_from_ms()

    print(f"[STOCK] Пропущено (нет в Ozon): {skipped}")
    print(f"[STOCK] Передаём в Ozon позиций: {len(stocks)}")
    print(f"[STOCK] Строк в отчёте CSV: {len(report_rows)}")

    _send_stock_report_file(report_rows)

        if dry_run:
        print("[STOCK] DRY_RUN: обновление в Ozon не выполняется.")
        return

    if not stocks:
        print("[STOCK] Нет позиций для обновления.")
        return

    # Первый кабинет (Auto-MiX)
    update_stocks(stocks)

    # Второй кабинет (Trail Gear)
    try:
        update_stocks_ozon2(stocks)
    except Exception as e:
        msg = f"[STOCK] Ошибка обновления остатков во втором кабинете Ozon: {e!r}"
        print(msg)
        try:
            send_telegram_message(msg)
        except Exception:
            pass

if __name__ == "__main__":
    main()
