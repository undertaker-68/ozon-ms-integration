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
      - Товары из IGNORE_STOCK_OFF
