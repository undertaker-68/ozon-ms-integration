import os
import csv
import tempfile
from typing import Dict, List, Tuple, Set

from dotenv import load_dotenv

from ms_client import get_stock_all
from ozon_client import (
    get_products_state_by_offer_ids as get_products_state_by_offer_ids_ozon1,
    update_stocks as update_stocks_ozon1,
)
from ozon_client2 import (
    get_products_state_by_offer_ids as get_products_state_by_offer_ids_ozon2,
    update_stocks as update_stocks_ozon2,
)

try:
    from notifier import send_telegram_message, send_telegram_document
except ImportError:
    def send_telegram_message(text: str) -> bool:
        print("Telegram notifier не доступен:", text)
        return False

    def send_telegram_document(file_path: str, caption: str = "") -> bool:
        print("Telegram notifier (document) не доступен:", file_path, caption)
        return False


load_dotenv()

# ---------------------
#  РЕЖИМ РАБОТЫ
# ---------------------

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


# ---------------------
#  ИГНОРИРУЕМЫЕ АРТИКУЛЫ
# ---------------------

def _parse_ignore_offers() -> Set[str]:
    raw = os.getenv("IGNORE_STOCK_OFFERS", "").strip()
    ignore: Set[str] = set()
    if not raw:
        return ignore
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        ignore.add(normalize_article(part))
    return ignore


IGNORE_STOCK_OFFERS: Set[str] = _parse_ignore_offers()


# ---------------------
#  КАРТА СКЛАДОВ
# ---------------------

def _parse_warehouse_map() -> Dict[str, int]:
    """
    Возвращает соответствие:
      { ms_store_id (UUID МойСклад) -> ozon_warehouse_id (int) }
    """
    warehouse_map: Dict[str, int] = {}

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
                print(f"[WARN] Неверный формат пары складов в OZON_WAREHOUSE_MAP: {pair!r}")

    # старый вариант для совместимости
    if not warehouse_map:
        ms_old = os.getenv("MS_OZON_STORE_ID")
        wh_old = os.getenv("OZON_WAREHOUSE_ID")
        if ms_old and wh_old:
            try:
                warehouse_map[ms_old] = int(wh_old)
            except Exception:
                print(f"[WARN] Неверные значения MS_OZON_STORE_ID/OZON_WAREHOUSE_ID: {ms_old!r}/{wh_old!r}")

    if not warehouse_map:
        raise RuntimeError("Не заданы склады. Укажи OZON_WAREHOUSE_MAP в .env")

    print("[STOCK] Карта складов:")
    for ms_id, wh_id in warehouse_map.items():
        print(f"  MS store {ms_id} → Ozon warehouse_id {wh_id}")

    return warehouse_map


WAREHOUSE_MAP: Dict[str, int] = _parse_warehouse_map()


# ---------------------
#  ЧТЕНИЕ ОСТАТКОВ ИЗ МОЙСКЛАД
# ---------------------

def _fetch_ms_stock_rows_for_store(ms_store_id: str, page_limit: int = 1000) -> List[dict]:
    """
    Возвращает полный список позиций ассортимента для одного склада МойСклад.
    Использует /entity/assortment через ms_client.get_stock_all.
    """
    rows: List[dict] = []
    offset = 0

    while True:
        data = get_stock_all(limit=page_limit, offset=offset, store_id=ms_store_id)
        batch = data.get("rows") or []
        if not batch:
            break

        rows.extend(batch)

        if len(batch) < page_limit:
            break

        offset += page_limit

    return rows


# ---------------------
#  ПОСТРОЕНИЕ СПИСКА ДЛЯ ВЫГРУЗКИ В OZON
# ---------------------

def build_ozon_stocks_from_ms() -> Tuple[List[dict], List[dict], int, List[dict]]:
    """
    Читаем остатки из МойСклад и фильтруем по статусам товаров в Ozon.

    Возвращаем:
      stocks_ozon1   – список для API /v2/products/stocks (кабинет Auto-MiX)
      stocks_ozon2   – список для API /v2/products/stocks (кабинет Trail Gear)
      skipped_count  – сколько позиций отфильтровано (архив/нет в Ozon)
      report_rows    – строки для CSV-отчёта (общий список по обоим кабинетам)
    """
    # ---------- Собираем кандидатов из МойСклад ----------

    candidates: List[Tuple[str, int, int]] = []  # (article, stock_int, ozon_wh_id)
    names_by_article: Dict[str, str] = {}

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
        print("[STOCK] Нет позиций для обработки (кандидаты пусты).")
        return [], [], 0, []

    # ---------- Получаем статусы товаров из обоих кабинетов Ozon ----------

    offer_ids = sorted({article for article, _, _ in candidates})

    # кабинет 1 (Auto-MiX)
    try:
        ozon1_states = get_products_state_by_offer_ids_ozon1(offer_ids) or {}
    except Exception as e:
        msg = (
            "❗ Не удалось получить статусы из Ozon #1 (Auto-MiX): "
            f"{e!r}. Все товары временно считаем активными."
        )
        print(msg)
        try:
            send_telegram_message(msg)
        except Exception:
            pass
        ozon1_states = {oid: "ACTIVE" for oid in offer_ids}

    # кабинет 2 (Trail Gear)
    try:
        ozon2_states = get_products_state_by_offer_ids_ozon2(offer_ids) or {}
    except Exception as e:
        msg = (
            "❗ Не удалось получить статусы из Ozon #2 (Trail Gear): "
            f"{e!r}. Все товары временно считаем активными."
        )
        print(msg)
        try:
            send_telegram_message(msg)
        except Exception:
            pass
        ozon2_states = {oid: "ACTIVE" for oid in offer_ids}

    def is_allowed(offer_id: str) -> bool:
        """
        Правила:
          - если в ОДНОМ из кабинетов товар ARCHIVED → полностью исключаем;
          - если в обоих кабинетах товар не найден (None) → исключаем;
          - иначе товар считаем живым и допускаем к выгрузке.
        """
        s1 = ozon1_states.get(offer_id)
        s2 = ozon2_states.get(offer_id)

        if s1 == "ARCHIVED" or s2 == "ARCHIVED":
            return False

        if s1 is None and s2 is None:
            return False

        return True

    # ---------- Фильтрация кандидатов по статусам ----------

    filtered_candidates: List[Tuple[str, int, int]] = []
    skipped_total = 0

    for article, stock, ozon_wh_id in candidates:
        if not is_allowed(article):
            skipped_total += 1
            continue
        filtered_candidates.append((article, stock, ozon_wh_id))

    if not filtered_candidates:
        print("[STOCK] После фильтрации по статусам в Ozon не осталось ни одной позиции.")
        return [], [], skipped_total, []

    # ---------- Формируем итоговые данные ----------

    stocks_ozon1: List[dict] = []
    stocks_ozon2: List[dict] = []
    report_rows: List[dict] = []
    seen_in_report: Set[str] = set()

    for article, stock, ozon_wh_id in filtered_candidates:
        stock_entry = {
            "offer_id": article,
            "stock": stock,
            "warehouse_id": ozon_wh_id,
        }

        # Для кабинета 1 — только если товар там существует и не архивный
        s1 = ozon1_states.get(article)
        if s1 and s1 != "ARCHIVED":
            stocks_ozon1.append(stock_entry)

        # Для кабинета 2 — только если товар там существует и не архивный
        s2 = ozon2_states.get(article)
        if s2 and s2 != "ARCHIVED":
            stocks_ozon2.append(stock_entry)

        # Для отчёта достаточно одного раза на артикул
        if article not in seen_in_report:
            report_rows.append(
                {
                    "name": names_by_article.get(article, ""),
                    "article": article,
                    "stock": stock,
                }
            )
            seen_in_report.add(article)

    return stocks_ozon1, stocks_ozon2, skipped_total, report_rows


# ---------------------
#  ГЕНЕРАЦИЯ CSV-ОТЧЁТА
# ---------------------

def send_stock_report_csv(report_rows: List[dict]) -> None:
    """
    Формирует единый CSV-файл по обоим кабинетам и отправляет его в Telegram.
    """
    if not report_rows:
        print("[STOCK] Нет строк для отчёта CSV, пропускаем отправку файла.")
        return

    print(f"[STOCK] Строк в отчёте CSV: {len(report_rows)}")

    fd, tmp_path = tempfile.mkstemp(prefix="ozon_stock_", suffix=".csv")
    os.close(fd)

    try:
        with open(tmp_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(["№", "Наименование", "Артикул", "Кол-во"])

            for idx, row in enumerate(report_rows, start=1):
                writer.writerow([
                    idx,
                    row.get("name", ""),
                    row.get("article", ""),
                    row.get("stock", 0),
                ])

        ok = send_telegram_document(
            tmp_path,
            caption="Остатки Ozon (Auto-MiX + Trail Gear)",
        )
        if ok:
            print(f"[STOCK] CSV отправлен: {tmp_path}")
        else:
            print(f"[STOCK] Не удалось отправить CSV в Telegram: {tmp_path}")
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


# ---------------------
#  MAIN
# ---------------------

def main() -> None:
    print(f"[STOCK] DRY_RUN={DRY_RUN}")

    # Уведомление о запуске по крону
    try:
        send_telegram_message(f"🔁 CRON: запуск sync_stock (остатки), DRY_RUN={DRY_RUN}")
    except Exception as e:
        print("Ошибка отправки уведомления о запуске в Telegram:", e)

    stocks_ozon1, stocks_ozon2, skipped, report_rows = build_ozon_stocks_from_ms()

    print(f"[STOCK] Пропущено (по статусам/нет в Ozon): {skipped}")
    print(
        f"[STOCK] Передаём в Ozon позиций: "
        f"{len(stocks_ozon1)} (кабинет 1), {len(stocks_ozon2)} (кабинет 2)"
    )

    # Единый отчёт (по обоим кабинетам)
    send_stock_report_csv(report_rows)

    if DRY_RUN:
        print("[STOCK] DRY_RUN=True – обновление остатков в Ozon НЕ выполняется.")
        return

    # --- Первый кабинет (Auto-MiX) ---
    if stocks_ozon1:
        try:
            print(f"[OZON] Обновление остатков, позиций: {len(stocks_ozon1)} (кабинет 1)")
            update_stocks_ozon1(stocks_ozon1)
        except Exception as e:
            msg = f"[STOCK] Ошибка обновления остатков в первом кабинете Ozon: {e!r}"
            print(msg)
            try:
                send_telegram_message(msg)
            except Exception:
                pass
    else:
        print("[OZON] Для первого кабинета нет позиций для обновления остатков.")

    # --- Второй кабинет (Trail Gear) ---
    if stocks_ozon2:
        try:
            print(f"[OZON2] Обновление остатков, позиций: {len(stocks_ozon2)} (кабинет 2)")
            update_stocks_ozon2(stocks_ozon2)
        except Exception as e:
            msg = f"[STOCK] Ошибка обновления остатков во втором кабинете Ozon: {e!r}"
            print(msg)
            try:
                send_telegram_message(msg)
            except Exception:
                pass
    else:
        print("[OZON2] Для второго кабинета нет позиций для обновления остатков.")


if __name__ == "__main__":
    main()
