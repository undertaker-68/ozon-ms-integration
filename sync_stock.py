# sync_stock.py
import os
import csv
import tempfile
from typing import Dict, List, Set

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

# ========================== НАСТРОЙКИ ==========================
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
ENABLE_OZON2_STOCKS = os.getenv("ENABLE_OZON2_STOCKS", "true").lower() == "true"

OZON2_WAREHOUSE_ID = None
if os.getenv("OZON2_WAREHOUSE_ID"):
    try:
        OZON2_WAREHOUSE_ID = int(os.getenv("OZON2_WAREHOUSE_ID"))
    except ValueError:
        print("[WARN] OZON2_WAREHOUSE_ID не число, будет игнорироваться")

IGNORE_OFFERS_ENV = os.getenv("IGNORE_STOCK_OFFERS", "")
IGNORE_STOCK_OFFERS: Set[str] = {x.strip() for x in IGNORE_OFFERS_ENV.split(",") if x.strip()}

# Поддержка старого и нового формата складов
def _parse_warehouse_map() -> Dict[str, int]:
    warehouse_map: Dict[str, int] = {}

    raw = os.getenv("OZON_WAREHOUSE_MAP", "").strip()
    if raw:
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                ms_id, ozon_id_str = part.split(":", 1)
                warehouse_map[ms_id.strip()] = int(ozon_id_str.strip())
            except Exception as e:
                print(f"[WARN] Неверный формат в OZON_WAREHOUSE_MAP: {part} — {e}")

    # Поддержка старых переменных
    if not warehouse_map:
        ms_id = os.getenv("MS_OZON_STORE_ID")
        ozon_id = os.getenv("OZON_WAREHOUSE_ID")
        if ms_id and ozon_id:
            try:
                warehouse_map[ms_id.strip()] = int(ozon_id.strip())
            except ValueError:
                pass

    return warehouse_map

WAREHOUSE_MAP = _parse_warehouse_map()

if not WAREHOUSE_MAP:
    raise RuntimeError("Не настроены склады: укажите OZON_WAREHOUSE_MAP или MS_OZON_STORE_ID + OZON_WAREHOUSE_ID")

# ========================== ЛОГИКА ==========================

def _get_available_stock(row: dict) -> int:
    """Доступный остаток = stock - reserve"""
    stock = int(row.get("stock") or 0)
    reserve = int(row.get("reserve") or 0)
    return max(stock - reserve, 0)


def build_ozon_stocks_from_ms():
    """
    Основная логика:
    - Берём остатки ТОЛЬКО с нужных складов МоегоСклада (по WAREHOUSE_MAP)
    - Для комплектов — считаем минимальное доступное количество по компонентам
    - Фильтруем по статусу в Ozon (не ARCHIVED)
    """
    all_stocks_ozon1: List[dict] = []
    all_stocks_ozon2: List[dict] = []
    report_rows: List[dict] = []
    skipped_count = 0

    # Собираем все нужные store_id из МоегоСклада
    ms_store_ids = list(WAREHOUSE_MAP.keys())

    print(f"[STOCK] Обрабатываем склады МоегоСклада: {ms_store_ids}")

    # Словарь: href товара → доступный остаток (по всем нужным складам суммируем)
    stock_by_href: Dict[str, int] = {}

    # Сначала собираем остатки по всем нужным складам
    for ms_store_id in ms_store_ids:
        print(f"[MS] Загружаем остатки со склада ID={ms_store_id}")
        offset = 0
        limit = 1000

        while True:
            data = get_stock_all(limit=limit, offset=offset, store_id=ms_store_id)
            rows = data.get("rows", [])
            if not rows:
                break

            for row in rows:
                meta = row.get("meta", {})
                href = meta.get("href")
                if not href:
                    continue

                available = _get_available_stock(row)

                # Если это комплект — пересчитываем по компонентам
                if row.get("bundle") is True or "components" in row:
                    available = compute_bundle_available(row, stock_by_href)

                stock_by_href[href] = stock_by_href.get(href, 0) + available

            if len(rows) < limit:
                break
            offset += limit

    print(f"[STOCK] Собрано остатков по {len(stock_by_href)} позициям (с учётом комплектов)")

    # Теперь пробегаем по всем позициям ассортимента (чтобы получить артикулы)
    # Берём с любого склада — нам нужны только мета и артикул
    sample_store_id = ms_store_ids[0]
    offset = 0
    limit = 1000
    offer_ids_to_check = []

    while True:
        data = get_stock_all(limit=limit, offset=offset, store_id=sample_store_id)
        rows = data.get("rows", [])
        if not rows:
            break

        for row in rows:
            article = row.get("article") or row.get("code")
            if not article:
                continue
            article = str(article).strip()

            if article in IGNORE_STOCK_OFFERS:
                continue

            meta_href = row.get("meta", {}).get("href")
            if not meta_href:
                continue

            available = stock_by_href.get(meta_href, 0)
            name = row.get("name", "")

            # Собираем артикулы для проверки статуса в Ozon
            offer_ids_to_check.append(article)

            # Определяем, в какой кабинет отправлять
            ozon_wh_id = WAREHOUSE_MAP.get(sample_store_id)  # можно улучшить под мультисклады

            st1 = None
            st2 = None
            if offer_ids_to_check:  # будем проверять батчами ниже
                pass

            report_rows.append({
                "name": name,
                "article": article,
                "stock": available,
            })

        if len(rows) < limit:
            break
        offset += limit

    # Пакетная проверка статуса товаров в Ozon
    print(f"[OZON] Проверяем статус {len(offer_ids_to_check)} товаров...")
    state_ozon1 = get_products_state_by_offer_ids_ozon1(offer_ids_to_check)
    state_ozon2 = get_products_state_by_offer_ids_ozon2(offer_ids_to_check) if ENABLE_OZON2_STOCKS else {}

    # Финальная сборка
    for row in report_rows:
        article = row["article"]
        stock = row["stock"]

        if article in IGNORE_STOCK_OFFERS:
            skipped_count += 1
            continue

        st1 = state_ozon1.get(article)
        st2 = state_ozon2.get(article) if ENABLE_OZON2_STOCKS else None

        send_to_ozon1 = st1 == "ACTIVE"
        send_to_ozon2 = ENABLE_OZON2_STOCKS and st2 == "ACTIVE" and OZON2_WAREHOUSE_ID is not None

        if not send_to_ozon1 and not send_to_ozon2:
            skipped_count += 1
            continue

        # Берём warehouse_id из маппинга (пока один склад)
        ozon_wh_id = next(iter(WAREHOUSE_MAP.values()))

        if send_to_ozon1:
            all_stocks_ozon1.append({
                "offer_id": article,
                "stock": stock,
                "warehouse_id": ozon_wh_id,
            })

        if send_to_ozon2 and OZON2_WAREHOUSE_ID:
            all_stocks_ozon2.append({
                "offer_id": article,
                "stock": stock,
                "warehouse_id": OZON2_WAREHOUSE_ID,
            })

    print(f"[STOCK] Готово к отправке: Ozon1={len(all_stocks_ozon1)}, Ozon2={len(all_stocks_ozon2)}, пропущено={skipped_count}")

    return all_stocks_ozon1, all_stocks_ozon2, skipped_count, report_rows


def write_csv_report(rows: List[dict]) -> str:
    fd, path = tempfile.mkstemp(prefix="stock_sync_", suffix=".csv")
    os.close(fd)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["№", "Название", "Артикул", "Остаток (FBS)"])
        for i, r in enumerate(rows, 1):
            writer.writerow([i, r["name"], r["article"], r["stock"]])
    return path


def main(dry_run_override: bool = None):
    global DRY_RUN
    if dry_run_override is not None:
        DRY_RUN = dry_run_override

    print(f"[STOCK] Запуск синхронизации остатков (DRY_RUN={DRY_RUN})")

    stocks1, stocks2, skipped, report = build_ozon_stocks_from_ms()

    # Отчёт
    try:
        path = write_csv_report(report)
        send_telegram_document(path, caption=f"Остатки → Ozon (FBS склад)\nОтправлено: {len(stocks1)} + {len(stocks2)}")
        os.unlink(path)
    except Exception as e:
        print(f"[ERROR] Не удалось отправить отчёт: {e}")

    if DRY_RUN:
        print("[DRY_RUN] Остатки в Ozon НЕ обновлены")
        return

    # Отправка в Ozon
    if stocks1:
        print(f"[OZON1] Отправка {len(stocks1)} остатков...")
        update_stocks_ozon1(stocks1)

    if ENABLE_OZON2_STOCKS and stocks2:
        print(f"[OZON2] Отправка {len(stocks2)} остатков...")
        update_stocks_ozon2(stocks2)

    print("[STOCK] Синхронизация остатков завершена")


if __name__ == "__main__":
    main()
