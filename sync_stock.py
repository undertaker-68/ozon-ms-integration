import os
import csv
import tempfile
from typing import Dict, List, Tuple, Set

from dotenv import load_dotenv

from ms_client import get_stock_all, compute_bundle_available
from ozon_client import (
    get_products_state_by_offer_ids as get_products_state_by_offer_ids_ozon1,
    update_stocks as update_stocks_ozon1,
)
from ozon_client2 import (
    get_products_state_by_offer_ids as get_products_state_by_offer_ids_ozon2,
    update_stocks as update_stocks_ozon2,
)
from notifier import send_telegram_message, send_telegram_document

load_dotenv()

# --------------------------
# НАСТРОЙКИ
# --------------------------

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
# включение/выключение обновления второго кабинета (по желанию)
ENABLE_OZON2_STOCKS = os.getenv("ENABLE_OZON2_STOCKS", "true").lower() == "true"

# отдельный склад для второго кабинета
OZON2_WAREHOUSE_ID_ENV = os.getenv("OZON2_WAREHOUSE_ID")
OZON2_WAREHOUSE_ID = int(OZON2_WAREHOUSE_ID_ENV) if OZON2_WAREHOUSE_ID_ENV else None


def _parse_ignore_offers() -> Set[str]:
    raw = os.getenv("IGNORE_STOCK_OFFERS", "")
    result: Set[str] = set()
    for part in raw.split(","):
        part = part.strip()
        if part:
            result.add(part)
    return result


IGNORE_STOCK_OFFERS: Set[str] = _parse_ignore_offers()


def _parse_warehouse_map() -> Dict[str, int]:
    """
    OZON_WAREHOUSE_MAP = "MS_STORE_ID:OZON_WAREHOUSE_ID,MS_STORE_ID2:OZON_WAREHOUSE_ID2"
    """
    warehouse_map: Dict[str, int] = {}

    raw_map = os.getenv("OZON_WAREHOUSE_MAP", "")
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
            except ValueError:
                print(f"[WARN] Неверный OZON_WAREHOUSE_ID: {wh_old!r}")

    if not warehouse_map:
        raise RuntimeError("Не задан OZON_WAREHOUSE_MAP / MS_OZON_STORE_ID / OZON_WAREHOUSE_ID в .env")

    return warehouse_map


WAREHOUSE_MAP: Dict[str, int] = _parse_warehouse_map()


def normalize_article(article: str) -> str:
    """
    Нормализуем артикул:
      - убираем пробелы по краям
      - приводим к строке
    """
    if article is None:
        return ""
    return str(article).strip()


def _fetch_ms_stock_rows_for_store(store_id: str) -> List[dict]:
    """
    Читаем все строки ассортимента по складу store_id.
    """
    rows: List[dict] = []
    limit = 1000
    offset = 0

    while True:
        data = get_stock_all(limit=limit, offset=offset, store_id=store_id)
        batch = data.get("rows") or []
        if not batch:
            break

        rows.extend(batch)
        if len(batch) < limit:
            break

        offset += limit

    print(f"[MS] Получено {len(rows)} строк ассортимента по складу {store_id}")
    return rows


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
        print(f"[MS] Обработка склада MS store_id={ms_store_id} → Ozon warehouse_id={ozon_wh_id}")

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

            # Определяем тип ассортимента: обычный товар или комплект (bundle)
            meta = row.get("meta") or {}
            item_type = meta.get("type")

        if item_type == "bundle":
            # Для комплектов считаем доступный остаток по компонентам
            stock_int = compute_bundle_available(row)
        else:
            # Для обычных товаров сначала пробуем 'quantity' (Доступно), если его нет – 'stock'
            stock_raw = row.get("quantity")
            if stock_raw is None:
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

    print(f"[OZON] Запрашиваем статусы {len(offer_ids)} товаров (кабинет 1)...")
    states_ozon1_raw = get_products_state_by_offer_ids_ozon1(offer_ids)
    print(f"[OZON2] Запрашиваем статусы {len(offer_ids)} товаров (кабинет 2)...")
    states_ozon2_raw = get_products_state_by_offer_ids_ozon2(offer_ids)

    # get_products_state_by_offer_ids возвращает словарь { offer_id: "ACTIVE"/"ARCHIVED"/None }
    # Нормализуем ключи так же, как артикула из МойСклад
    state_by_offer_ozon1: Dict[str, str | None] = {
        normalize_article(oid): state for oid, state in (states_ozon1_raw or {}).items()
    }
    state_by_offer_ozon2: Dict[str, str | None] = {
        normalize_article(oid): state for oid, state in (states_ozon2_raw or {}).items()
    }

    # ---------- Фильтруем кандидатов по статусам в кабинетах ----------

    stocks_ozon1: List[dict] = []
    stocks_ozon2: List[dict] = []
    skipped_count = 0
    report_rows: List[dict] = []

    for article, stock, ozon_wh_id in candidates:
        st1_state = state_by_offer_ozon1.get(article)
        st2_state = state_by_offer_ozon2.get(article)

        # Если нет ни в одном кабинете – пропускаем
        if st1_state is None and st2_state is None:
            skipped_count += 1
            continue

        send_to_ozon1 = (st1_state == "ACTIVE")
        send_to_ozon2 = (st2_state == "ACTIVE")

        # Если в кабинетах только ARCHIVED/None – пропускаем
        if not send_to_ozon1 and not send_to_ozon2:
            skipped_count += 1
            continue

        if send_to_ozon1:
            stocks_ozon1.append(
                {
                    "offer_id": article,
                    "stock": stock,
                    "warehouse_id": ozon_wh_id,  # первый кабинет – по карте WAREHOUSE_MAP
                }
            )

        if send_to_ozon2:
            wh2 = OZON2_WAREHOUSE_ID if OZON2_WAREHOUSE_ID is not None else ozon_wh_id
            stocks_ozon2.append(
                {
                    "offer_id": article,
                    "stock": stock,
                    "warehouse_id": wh2,  # второй кабинет – либо отдельный ID, либо тот же
                }
            )

        report_rows.append(
            {
                "name": names_by_article.get(article, ""),
                "article": article,
                "stock": stock,
            }
        )

    print(
        f"[STOCK] После фильтрации: "
        f"к отправке в Ozon1: {len(stocks_ozon1)}, "
        f"к отправке в Ozon2: {len(stocks_ozon2)}, "
        f"отфильтровано: {skipped_count}"
    )

    return stocks_ozon1, stocks_ozon2, skipped_count, report_rows


def write_csv_report(report_rows: List[dict]) -> str:
    """
    Пишем CSV во временный файл, возвращаем путь.
    """
    fd, path = tempfile.mkstemp(prefix="stock_report_", suffix=".csv")
    os.close(fd)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["#", "Название", "Артикул", "Остаток"])
        for idx, row in enumerate(report_rows, start=1):
            writer.writerow(
                [
                    idx,
                    row.get("name", ""),
                    row.get("article", ""),
                    row.get("stock", 0),
                ]
            )

    return path


def main():
    print("[STOCK] Запуск обновления остатков...")

    stocks_ozon1, stocks_ozon2, skipped_count, report_rows = build_ozon_stocks_from_ms()

    # ---------- Отчёт в Telegram ----------

    try:
        csv_path = write_csv_report(report_rows)
        send_telegram_document(csv_path, caption="Отчёт по остаткам (МойСклад → Ozon)")
        os.remove(csv_path)
    except Exception as e:
        print(f"[STOCK] Не удалось отправить CSV-отчёт в Telegram: {e!r}")

    if DRY_RUN:
        print("[STOCK] DRY_RUN=true — обновление остатков в Ozon не выполняется.")
        return

    # ---------- Обновление остатков в Ozon (кабинет 1) ----------

    if stocks_ozon1:
        try:
            print(f"[OZON1] Обновление остатков, позиций: {len(stocks_ozon1)}")
            update_stocks_ozon1(stocks_ozon1)
        except Exception as e:
            msg = f"[STOCK] Ошибка обновления остатков в первом кабинете Ozon: {e!r}"
            print(msg)
            try:
                send_telegram_message(msg)
            except Exception:
                pass
    else:
        print("[OZON1] Для первого кабинета нет позиций для обновления остатков.")

    # ---------- Обновление остатков в Ozon (кабинет 2) ----------

    if stocks_ozon2:
        try:
            print(f"[OZON2] Обновление остатков, позиций: {len(stocks_ozon2)}")
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
