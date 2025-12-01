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


# ---------------------
#  НОРМАЛИЗАЦИЯ АРТИКУЛОВ
# ---------------------

# Замена русских букв на английские
ARTICLE_TRANSLATION = str.maketrans({
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M",
    "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T",
    "У": "Y", "Х": "X",

    "а": "a", "в": "b", "е": "e", "к": "k", "м": "m",
    "н": "h", "о": "o", "р": "p", "с": "c", "т": "t",
    "у": "y", "х": "x",
})

def normalize_article(article: str) -> str:
    """Приводим артикула к единой раскладке."""
    return article.translate(ARTICLE_TRANSLATION).strip()


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
#  ФИЛЬТРАЦИЯ + НОРМА АРТИКУЛОВ
# ---------------------

def _is_archive_or_deleted(row: dict) -> bool:
    """Не используем товары «В архиве» или «Сняты с продажи»."""
    assortment = row.get("assortment") or {}
    a_state = (assortment.get("archived") or False)
    status = (assortment.get("status") or "").lower().strip()

    return (
        a_state is True
        or status in ("archived", "removed", "discontinued", "snyat_s_prodazhi", "снят с продажи", "снят с продаж")
    )


def build_ozon_stocks_from_ms() -> tuple[list[dict], int, list[dict]]:
    candidates: list[tuple[str, int, int]] = []
    names_by_article: dict[str, str] = {}

    for ms_store_id, ozon_wh_id in WAREHOUSE_MAP.items():
        print(f"[STOCK] Читаем остатки из МС: store_id={ms_store_id} → Ozon warehouse_id={ozon_wh_id}")

        rows = _fetch_ms_stock_rows_for_store(ms_store_id)

        for row in rows:

            # ❌ Пропускаем архив / сняты с продажи
            if _is_archive_or_deleted(row):
                continue

            article_raw = row.get("article")
            if not article_raw:
                continue

            # ✔️ нормализуем артикул
            article = normalize_article(article_raw)

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

    # ============ ФИЛЬТРАЦИЯ ПО СТАТУСАМ OZON (оба кабинета) ============

    # нормализованный список offer_id
    offer_ids = [c[0] for c in candidates]

    # статусы первого кабинета
    from ozon_client import get_products_state_by_offer_ids as ozon1_states_fetch
    ozon1_states = ozon1_states_fetch(offer_ids) or []

    # статусы второго кабинета
    from ozon_client2 import get_products_state_by_offer_ids as ozon2_states_fetch
    ozon2_states = ozon2_states_fetch(offer_ids) or []

    # Создаём карту offer_id → state (берём самое «жёсткое» состояние)
    status_map = {}

    def merge_state(offer_id, state):
        if not offer_id or not state:
            return
    # Приоритет: archived > disabled > unavailable > available
        prior = {
            "archived": 3,
            "disabled": 2,
            "unavailable": 1,
            "available": 0,
            None: -1,
        }
        prev = status_map.get(offer_id)
        if prev is None or prior[state] > prior.get(prev, -1):
            status_map[offer_id] = state

    for item in ozon1_states:
        merge_state(item.get("offer_id"), item.get("state"))

    for item in ozon2_states:
        merge_state(item.get("offer_id"), item.get("state"))

    # Теперь фильтруем кандидатов
    filtered_candidates = []
    for article, stock, wh in candidates:
        state = status_map.get(article, "available")
        if state in ("archived", "disabled", "unavailable"):
            # не включаем
            continue
        filtered_candidates.append((article, stock, wh))

    candidates = filtered_candidates
# ============================================================

    stocks: list[dict] = []
    skipped_not_found = 0

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

    return stocks, skipped_not_found, report_rows



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
        print(f"[STOCK] CSV отправлен: {tmp_path}" if ok else f"[STOCK] Ошибка отправки CSV: {tmp_path}")

    finally:
        try:
            os.remove(tmp_path)
        except:
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
        except:
            pass


if __name__ == "__main__":
    main()
