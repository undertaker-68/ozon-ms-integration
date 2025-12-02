import os
import csv
import tempfile
import time
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

# ---------------------
#  НОРМАЛИЗАЦИЯ АРТИКУЛОВ (RU → EN)
# ---------------------

ARTICLE_TRANSLATION = str.maketrans({
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M",
    "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T",
    "У": "Y", "Х": "X",
    "а": "a", "в": "b", "е": "e", "к": "k", "м": "m",
    "н": "h", "о": "o", "р": "p", "с": "c", "т": "t",
    "у": "y", "х": "x",
})


def normalize_article(article: str) -> str:
    """Привести артикул к единой раскладке для сопоставления с Ozon."""
    if not isinstance(article, str):
        article = str(article)
    return article.translate(ARTICLE_TRANSLATION).strip()


IGNORE_STOCK_OFFERS = set(
    normalize_article(offer.strip())
    for offer in os.getenv("IGNORE_STOCK_OFFERS", "").split(",")
    if offer.strip()
)


# ---------------------
#  СКЛАДЫ
# ---------------------

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

    # старый вариант для совместимости
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


# ---------------------
#  ЧТЕНИЕ ОСТАТКОВ ИЗ МС
# ---------------------

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


# ---------------------
#  СБОР ОСТАТКОВ + ФИЛЬТРАЦИЯ ПО СТАТУСАМ OZON
# ---------------------

def build_ozon_stocks_from_ms() -> tuple[list[dict], int, list[dict]]:
    """Читаем остатки из МойСклад и фильтруем по статусам товаров в Ozon.

    Возвращаем:
      stocks         – список для API /v2/products/stocks
      skipped_count  – сколько позиций отфильтровано (архив/нет в Ozon)
      report_rows    – строки для CSV-отчёта
    """
    candidates: list[tuple[str, int, int]] = []
    names_by_article: dict[str, str] = {}

    for ms_store_id, ozon_wh_id in WAREHOUSE_MAP.items():
        print(f"[STOCK] Читаем остатки из МС: store_id={ms_store_id} → Ozon warehouse_id={ozon_wh_id}")

        rows = _fetch_ms_stock_rows_for_store(ms_store_id)

        for row in rows:
            article_raw = row.get("article")
            if not article_raw:
                continue

            article = normalize_article(article_raw)
            if not article:
                continue

            if article in IGNORE_STOCK_OFFERS:
                continue

            name = (
                row.get("name")
                or (row.get("assortment") or {}).get("name")
                or ""
            )

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

    # ---------- ФИЛЬТРАЦИЯ ПО СТАТУСАМ ТОВАРОВ В OZON (оба кабинета) ----------

    offer_ids = sorted({c[0] for c in candidates})

    # первый кабинет – словарь {offer_id: "ARCHIVED"/"ACTIVE"/None}
    try:
        ozon1_states: dict[str, str | None] = get_products_state_by_offer_ids(offer_ids) or {}
    except Exception as e:
        print(f"[STOCK] Ошибка получения статусов товаров в первом кабинете Ozon: {e!r}")
        ozon1_states = {}

    # второй кабинет – такая же сигнатура
    try:
        from ozon_client2 import get_products_state_by_offer_ids as get_products_state_by_offer_ids_ozon2
        ozon2_states: dict[str, str | None] = get_products_state_by_offer_ids_ozon2(offer_ids) or {}
    except Exception as e:
        print(f"[STOCK] Не удалось получить статусы товаров во втором кабинете Ozon: {e!r}")
        ozon2_states = {}

    def is_allowed(oid: str) -> bool:
        """
        Разрешаем товар, если:
          - он существует хотя бы в одном кабинете (state != None)
          - и НИ в одном кабинете не помечен как ARCHIVED.
        Всё, чего нет ни в одном кабинете (оба None), или ARCHIVED – выкидываем.
        """
        s1 = ozon1_states.get(oid)
        s2 = ozon2_states.get(oid)

        # Нет ни в одном кабинете – считаем, что он не должен участвовать.
        if s1 is None and s2 is None:
            return False

        # В любом кабинете помечен как архивный – тоже выкидываем.
        if s1 == "ARCHIVED" or s2 == "ARCHIVED":
            return False

        return True

    filtered_candidates: list[tuple[str, int, int]] = []
    skipped_total = 0

    for article, stock, ozon_wh_id in candidates:
        if not is_allowed(article):
            skipped_total += 1
            continue
        filtered_candidates.append((article, stock, ozon_wh_id))

    candidates = filtered_candidates

    # ---------- Формируем итоговые данные для выгрузки и отчёта ----------

    stocks: list[dict] = []
    for article, stock, ozon_wh_id in candidates:
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

    return stocks, skipped_total, report_rows


# ---------------------
#  ОТЧЁТ В TELEGRAM
# ---------------------

def _send_stock_report_file(report_rows: list[dict]) -> None:
    if not report_rows:
        print("[STOCK] Нет данных — CSV не создан.")
        return

    fd, tmp_path = tempfile.mkstemp(prefix="ozon_stock_", suffix=".csv")
    os.close(fd)

    try:
        with open(tmp_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(["№", "Наименование", "Артикул", "Кол-во"])

            for idx, row in enumerate(report_rows, start=1):
                writer.writerow([
                    idx,
                    row["name"],
                    row["article"],
                    row["stock"],
                ])

        ok = send_telegram_document(tmp_path, caption="Остатки Ozon (оба кабинета)")
        if ok:
            print(f"[STOCK] CSV отправлен: {tmp_path}")
        else:
            print(f"[STOCK] Ошибка отправки CSV: {tmp_path}")

    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


# ---------------------
#  ОСНОВНОЙ КОД
# ---------------------

def main(dry_run: bool | None = None) -> None:
    if dry_run is None:
        dry_run = DRY_RUN

    print(f"[STOCK] DRY_RUN={dry_run}")

    try:
        send_telegram_message(f"🔁 CRON: запуск sync_stock (остатки), DRY_RUN={dry_run}")
    except Exception:
        pass

    stocks, skipped, report_rows = build_ozon_stocks_from_ms()

    print(f"[STOCK] Пропущено (по статусам/нет в Ozon): {skipped}")
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

    # Небольшая пауза перед обновлением второго кабинета,
    # чтобы снизить риск TOO_MANY_REQUESTS
    time.sleep(1.5)

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
