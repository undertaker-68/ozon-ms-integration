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
ENABLE_OZON2_STOCKS = os.getenv("ENABLE_OZON2_STOCKS", "true").lower() == "true"

OZON2_WAREHOUSE_ID_ENV = os.getenv("OZON2_WAREHOUSE_ID")
OZON2_WAREHOUSE_ID = int(OZON2_WAREHOUSE_ID_ENV) if OZON2_WAREHOUSE_ID_ENV else None

IGNORE_OFFERS_ENV = os.getenv("IGNORE_STOCK_OFFERS", "")
IGNORE_STOCK_OFFERS: Set[str] = {
    x.strip() for x in IGNORE_OFFERS_ENV.split(",") if x.strip()
}


def _parse_warehouse_map() -> Dict[str, int]:
    """
    Разбираем OZON_WAREHOUSE_MAP или legacy-переменные
    и получаем словарь {ms_store_id: ozon_warehouse_id}
    """
    warehouse_map: Dict[str, int] = {}

    raw = os.getenv("OZON_WAREHOUSE_MAP", "") or ""
    raw = raw.strip()
    if raw:
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                ms_id, wh_id_str = part.split(":", 1)
            except ValueError:
                print(f"[WARN] Неверная пара в OZON_WAREHOUSE_MAP: {part!r}")
                continue
            ms_id = ms_id.strip()
            wh_id_str = wh_id_str.strip()
            if not ms_id or not wh_id_str:
                print(f"[WARN] Пустой ms_store_id или warehouse_id в паре {part!r}")
                continue
            try:
                wh_id = int(wh_id_str)
            except ValueError:
                print(f"[WARN] Неверный склад Ozon (не int) в паре {part!r}")
                continue
            warehouse_map[ms_id] = wh_id

    if not warehouse_map:
        ms_old = os.getenv("MS_OZON_STORE_ID")
        wh_old = os.getenv("OZON_WAREHOUSE_ID")
        if ms_old and wh_old:
            try:
                warehouse_map[ms_old] = int(wh_old)
            except ValueError:
                print(f"[WARN] Неверный OZON_WAREHOUSE_ID: {wh_old!r}")

    if not warehouse_map:
        raise RuntimeError(
            "Не задан OZON_WAREHOUSE_MAP / MS_OZON_STORE_ID / OZON_WAREHOUSE_ID в .env"
        )

    return warehouse_map


WAREHOUSE_MAP: Dict[str, int] = _parse_warehouse_map()


def normalize_article(article: str) -> str:
    if article is None:
        return ""
    return str(article).strip()


def _ms_calc_available(row: dict) -> int:
    """
    Передаваемый остаток:
      Остаток = stock - reserve

    Поля quantity / inTransit / ожидание НЕ используем.
    """
    stock_raw = row.get("stock")
    reserve_raw = row.get("reserve", 0)

    try:
        stock_val = int(stock_raw or 0)
    except Exception:
        stock_val = 0

    try:
        reserve_val = int(reserve_raw or 0)
    except Exception:
        reserve_val = 0

    available = stock_val - reserve_val
    return max(available, 0)


def _fetch_ms_stock_rows_for_store(store_id: str) -> List[dict]:
    """
    Читаем все строки ассортимента по конкретному складу store_id.
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
      stocks_ozon1   – список для /v2/products/stocks (кабинет 1)
      stocks_ozon2   – список для /v2/products/stocks (кабинет 2)
      skipped_count  – сколько позиций отфильтровано
      report_rows    – строки для CSV-отчёта
    """
    candidates: List[Tuple[str, int, int]] = []  # (article, stock_int, ozon_wh_id)
    names_by_article: Dict[str, str] = {}

    for ms_store_id, ozon_wh_id in WAREHOUSE_MAP.items():
        print(
            f"[MS] Обработка склада MS store_id={ms_store_id} → Ozon warehouse_id={ozon_wh_id}"
        )

        rows = _fetch_ms_stock_rows_for_store(ms_store_id)

        # карта остатков по href ассортимента для ЭТОГО склада
        # (Остаток = stock - reserve)
        stock_by_href: Dict[str, int] = {}
        for r in rows:
            href = None

            assort = r.get("assortment")
            if isinstance(assort, dict):
                meta = assort.get("meta") or {}
                href = meta.get("href")

            if not href:
                meta = r.get("meta") or {}
                href = meta.get("href")

            if not href:
                continue

            stock_by_href[href] = _ms_calc_available(r)

        # обрабатываем каждую строку ассортимента
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

            meta = row.get("meta") or {}
            item_type = meta.get("type")

            # Обычный товар: просто Остаток = stock - reserve
            if item_type != "bundle":
                stock_int = _ms_calc_available(row)
            else:
                # Комплект: считаем по компонентам, используя stock_by_href
                stock_int = compute_bundle_available(row, stock_by_href)

            stock_int = max(int(stock_int), 0)

            candidates.append((article, stock_int, ozon_wh_id))

            if article not in names_by_article and name:
                names_by_article[article] = name

    if not candidates:
        print("[STOCK] Нет позиций для обработки (кандидаты пусты).")
        return [], [], 0, []

    # ---------- Статусы товаров в Ozon ----------

    all_offer_ids = sorted({article for article, _, _ in candidates})
    print(f"[OZON] Всего артикулов для проверки: {len(all_offer_ids)}")

    states_ozon1_raw = get_products_state_by_offer_ids_ozon1(all_offer_ids)
    states_ozon2_raw = get_products_state_by_offer_ids_ozon2(all_offer_ids)

    state_by_offer_ozon1: Dict[str, str | None] = {
        normalize_article(oid): state
        for oid, state in (states_ozon1_raw or {}).items()
    }
    state_by_offer_ozon2: Dict[str, str | None] = {
        normalize_article(oid): state
        for oid, state in (states_ozon2_raw or {}).items()
    }

    # ---------- Фильтр по статусам ----------

    stocks_ozon1: List[dict] = []
    stocks_ozon2: List[dict] = []
    skipped_count = 0
    report_rows: List[dict] = []

    for article, stock, ozon_wh_id in candidates:
        st1_state = state_by_offer_ozon1.get(article)
        st2_state = state_by_offer_ozon2.get(article)

        if st1_state is None and st2_state is None:
            skipped_count += 1
            continue

        send_to_ozon1 = st1_state is not None and st1_state != "ARCHIVED"
        send_to_ozon2 = (
            ENABLE_OZON2_STOCKS
            and st2_state is not None
            and st2_state != "ARCHIVED"
        )

        if not send_to_ozon1 and not send_to_ozon2:
            skipped_count += 1
            continue

        name = names_by_article.get(article, "")

        if send_to_ozon1:
            stocks_ozon1.append(
                {
                    "offer_id": article,
                    "stock": stock,
                    "warehouse_id": ozon_wh_id,
                }
            )

        if send_to_ozon2 and OZON2_WAREHOUSE_ID:
            stocks_ozon2.append(
                {
                    "offer_id": article,
                    "stock": stock,
                    "warehouse_id": OZON2_WAREHOUSE_ID,
                }
            )

        report_rows.append(
            {
                "name": name,
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
    fd, path = tempfile.mkstemp(prefix="stock_report_", suffix=".csv")
    os.close(fd)

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
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

    # Отчёт в Telegram
    try:
        csv_path = write_csv_report(report_rows)
        send_telegram_document(
            csv_path, caption="Отчёт по остаткам (МойСклад → Ozon)"
        )
        os.remove(csv_path)
    except Exception as e:
        print(f"[STOCK] Не удалось отправить CSV-отчёт в Telegram: {e!r}")

    if DRY_RUN:
        print("[STOCK] DRY_RUN=true — обновление остатков в Ozon не выполняется.")
        return

    # Обновление 1-го кабинета
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
        print("[OZON1] Нет позиций для обновления остатков.")

    # Обновление 2-го кабинета
    if ENABLE_OZON2_STOCKS and stocks_ozon2:
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
