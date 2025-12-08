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
OZON2_MS_STORE_ID = os.getenv("OZON2_MS_STORE_ID", "").strip() or None
OZON2_WAREHOUSE_ID = os.getenv("OZON2_WAREHOUSE_ID", "").strip() or None

# игнорируемые артикулы (через запятую)
IGNORE_STOCK_OFFERS_RAW = os.getenv("IGNORE_STOCK_OFFERS", "") or ""
IGNORE_STOCK_OFFERS: Set[str] = {
    x.strip() for x in IGNORE_STOCK_OFFERS_RAW.split(",") if x.strip()
}


def _parse_warehouse_map() -> Dict[str, int]:
    """
    Разбираем OZON_WAREHOUSE_MAP или OZON_WAREHOUSE_ID / MS_OZON_STORE_ID
    и получаем словарь {ms_store_id: ozon_warehouse_id}
    """
    warehouse_map: Dict[str, int] = {}

    raw_map = os.getenv("OZON_WAREHOUSE_MAP", "").strip()
    if raw_map:
        # Формат: MS_STORE_ID:OZON_WAREHOUSE_ID[,MS_STORE_ID2:OZON_WAREHOUSE_ID2...]
        pairs = [p.strip() for p in raw_map.split(",") if p.strip()]
        for p in pairs:
            try:
                ms_store_id, wh_id_str = p.split(":", 1)
            except ValueError:
                print(f"[WARN] Неверная пара в OZON_WAREHOUSE_MAP: {p!r}")
                continue
            ms_store_id = ms_store_id.strip()
            wh_id_str = wh_id_str.strip()
            if not ms_store_id or not wh_id_str:
                print(f"[WARN] Пустой ms_store_id или warehouse_id в паре {p!r}")
                continue
            try:
                wh_id = int(wh_id_str)
            except ValueError:
                print(f"[WARN] Неверный Ozon warehouse_id (не int) в паре {p!r}")
                continue
            warehouse_map[ms_store_id] = wh_id

    # Бэкап: старые переменные MS_OZON_STORE_ID / OZON_WAREHOUSE_ID
    if not warehouse_map:
        ms_store_id = os.getenv("MS_OZON_STORE_ID", "").strip()
        wh_old = os.getenv("OZON_WAREHOUSE_ID", "").strip()
        if ms_store_id and wh_old:
            try:
                warehouse_map[ms_store_id] = int(wh_old)
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


def _ms_calc_available(row: dict) -> int:
    """
    Вычисляем передаваемый остаток по формуле:
      Остаток = stock - reserve

    Поля МойСклад:
      stock   — «Остаток» по выбранному складу
      reserve — «Резерв» по выбранному складу

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
    if available < 0:
        available = 0

    return available


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

    return rows


def build_ozon_stocks_from_ms() -> Tuple[List[dict], List[dict], int, List[List[str]]]:
    """
    Основная функция построения списка остатков для Ozon из МойСклад.

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

        # Строим карту остатков по href ассортимента для ЭТОГО склада
        stock_by_href: Dict[str, int] = {}
        for r in rows:
            href = None

            # Пытаемся взять href из r["assortment"]["meta"]["href"]
            assort = r.get("assortment")
            if isinstance(assort, dict):
                meta = assort.get("meta") or {}
                href = meta.get("href")

            # Если не нашли – пробуем r["meta"]["href"]
            if not href:
                meta = r.get("meta") or {}
                href = meta.get("href")

            if not href:
                continue

            # Остаток по формуле: Остаток - Резерв
            stock_by_href[href] = _ms_calc_available(r)

        # Теперь обрабатываем каждую строку и считаем stock_int
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
                # Для комплектов считаем доступный остаток по компонентам,
                # используя остатки по этому складу из stock_by_href
                try:
                    stock_int = compute_bundle_available(row, stock_by_href)
                except TypeError:
                    # Если compute_bundle_available ещё со старой сигнатурой (1 аргумент) –
                    # не падаем, но такие комплекты считаем недоступными.
                    stock_int = 0
            else:
                # Для обычных товаров считаем по формуле: Остаток - Резерв
                stock_int = _ms_calc_available(row)

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

    # get_products_state_by_offer_ids возвращает словарь offer_id -> объект состояния
    states_ozon1: Dict[str, dict] = states_ozon1_raw or {}
    states_ozon2: Dict[str, dict] = states_ozon2_raw or {}

    # ---------- Формируем итоговые списки остатков для Ozon ----------

    stocks_ozon1: List[dict] = []
    stocks_ozon2: List[dict] = []
    skipped_count = 0
    report_rows: List[List[str]] = []

    for article, stock_int, ozon_wh_id in candidates:
        state1 = states_ozon1.get(article)
        state2 = states_ozon2.get(article)

        # если товар не найден ни в одном кабинете – пропускаем
        if not state1 and not state2:
            skipped_count += 1
            continue

        name = names_by_article.get(article, "")

        # кабинет 1
        if state1:
            stocks_ozon1.append(
                {
                    "offer_id": article,
                    "stock": stock_int,
                    "warehouse_id": ozon_wh_id,
                }
            )

        # кабинет 2 (по желанию)
        if ENABLE_OZON2_STOCKS and state2:
            wh2_id = None
            if OZON2_MS_STORE_ID and OZON2_WAREHOUSE_ID and ms_store_id == OZON2_MS_STORE_ID:
                try:
                    wh2_id = int(OZON2_WAREHOUSE_ID)
                except ValueError:
                    wh2_id = None

            # если отдельный склад под второй кабинет не настроен – используем тот же склад
            if not wh2_id:
                wh2_id = ozon_wh_id

            stocks_ozon2.append(
                {
                    "offer_id": article,
                    "stock": stock_int,
                    "warehouse_id": wh2_id,
                }
            )

        report_rows.append(
            [
                article,
                name,
                str(stock_int),
                str(ozon_wh_id),
                "1" if state1 else "0",
                "1" if state2 else "0",
            ]
        )

    return stocks_ozon1, stocks_ozon2, skipped_count, report_rows


def main(dry_run: bool | None = None):
    """
    Точка входа для обновления остатков.
    """
    if dry_run is None:
        dry_run = DRY_RUN

    print(f"[STOCK] Запуск обновления остатков. DRY_RUN={dry_run}")

    stocks_ozon1, stocks_ozon2, skipped_count, report_rows = build_ozon_stocks_from_ms()

    print(f"[STOCK] Кандидатов для обновления: {len(report_rows)}, пропущено (нет в Ozon): {skipped_count}")
    print(f"[STOCK] Для кабинета 1: {len(stocks_ozon1)} позиций.")
    print(f"[STOCK] Для кабинета 2: {len(stocks_ozon2)} позиций.")

    # Формируем временный CSV-отчёт
    if report_rows:
        fd, tmp_path = tempfile.mkstemp(prefix="stock_report_", suffix=".csv")
        os.close(fd)
        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(["Артикул", "Название", "Остаток (stock-reserve)", "Склад Ozon", "В Ozon1", "В Ozon2"])
            writer.writerows(report_rows)

        try:
            send_telegram_document(tmp_path, caption="[STOCK] Отчёт по остаткам")
        except Exception as e:
            print(f"[WARN] Не удалось отправить отчёт в Telegram: {e!r}")
        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass

    if dry_run:
        print("[STOCK] DRY_RUN=TRUE, остатки в Ozon НЕ обновляем.")
        return

    # Обновляем остатки в первом кабинете
    if stocks_ozon1:
        try:
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

    # Обновляем остатки во втором кабинете (если включен)
    if ENABLE_OZON2_STOCKS and stocks_ozon2:
        try:
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
