import os
import csv
from datetime import datetime
import requests
import asyncio
from dotenv import load_dotenv
from ozon_client import get_fbs_postings as get_fbs_postings_ozon1
from ms_client import (
    find_product_by_article,
    create_customer_order,
    find_customer_order_by_name,
    update_customer_order_state,
    clear_reserve_for_order,
    create_demand_from_order,
)
from telegram import Bot

try:
    from notifier import send_telegram_message
except ImportError:
    from ozon_client2 import get_fbs_postings as get_fbs_postings_ozon2
except ImportError:
    get_fbs_postings_ozon2 = None
    
    def send_telegram_message(text: str) -> bool:
        print("Telegram notifier не доступен:", text)
        return False

load_dotenv()

DRY_RUN_ORDERS = os.getenv("DRY_RUN_ORDERS", "true").lower() == "true"

MS_STATE_AWAIT_PACK = os.getenv("MS_STATE_AWAIT_PACK")
MS_STATE_AWAIT_SHIP = os.getenv("MS_STATE_AWAIT_SHIP")
MS_STATE_DELIVERING = os.getenv("MS_STATE_DELIVERING")
MS_STATE_DELIVERED = os.getenv("MS_STATE_DELIVERED")
MS_STATE_CANCELLED = os.getenv("MS_STATE_CANCELLED")

MS_ORGANIZATION_HREF = os.getenv("MS_ORGANIZATION_HREF")
MS_AGENT_HREF = os.getenv("MS_AGENT_HREF")
MS_STORE_HREF = os.getenv("MS_STORE_HREF")

if not (MS_ORGANIZATION_HREF and MS_AGENT_HREF and MS_STORE_HREF):
    raise RuntimeError(
        "Не заданы MS_ORGANIZATION_HREF / MS_AGENT_HREF / MS_STORE_HREF. "
        "Скопируйте meta.href из МойСклад."
    )

ERRORS_FILE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "orders_errors.csv",
)

# Путь для отчёта по остаткам
STOCKS_REPORT_FILE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "stocks_report.csv",
)

def _human_error_from_exception(e: Exception) -> str:
    if isinstance(e, requests.HTTPError):
        resp = e.response
        status = resp.status_code if resp is not None else None
        data = None

        if resp is not None:
            try:
                data = resp.json()
            except Exception:
                data = None

        if status == 412 and isinstance(data, dict):
            errors = data.get("errors") or []
            if errors:
                err_msg = errors[0].get("error") or errors[0].get("message") or ""
                if "Нельзя отгрузить товар, которого нет на складе" in err_msg:
                    return (
                        "МойСклад: нельзя отгрузить товар, которого нет на складе "
                        "(остаток по складу Ozon = 0 или меньше)."
                    )
                return f"МойСклад вернул ошибку 412: {err_msg}"

        if isinstance(data, dict) and data.get("errors"):
            parts: list[str] = []
            for err in data["errors"]:
                msg = err.get("error") or err.get("message")
                if msg:
                    parts.append(msg)
            if parts:
                return f"HTTP {status or ''}: " + "; ".join(parts)

        return f"HTTP ошибка {status or ''} при обращении к API (подробности в логах)."

    msg = str(e)
    if "str' object does not support item assignment" in msg:
        return (
            "Внутренняя ошибка скрипта интеграции (TypeError: попытка изменить строку). "
            "Нужно исправить логику формирования данных перед отправкой."
        )

    return f"Неизвестная ошибка обработки отправления: {msg}"

def _append_order_errors_to_file(rows: list[dict]) -> None:
    """
    Записываем ошибки обработки заказов в CSV.
    Колонки: дата/время, номер заказа, артикул, название, причина ошибки.
    """
    if not rows:
        return

    file_exists = os.path.exists(ERRORS_FILE_PATH)

    with open(ERRORS_FILE_PATH, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        if not file_exists:
            writer.writerow(
                ["Дата/время", "Номер заказа", "Артикул", "Название", "Причина ошибки"]
            )

        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for r in rows:
            writer.writerow(
                [
                    now_str,
                    r.get("posting_number", ""),
                    r.get("article", ""),
                    r.get("name", ""),
                    r.get("reason", ""),
                ]
            )

def _build_error_rows_for_posting(posting: dict, reason: str) -> list[dict]:
    posting_number = posting.get("posting_number", "")
    products = posting.get("products") or []

    rows: list[dict] = []

    if products:
        for p in products:
            rows.append(
                {
                    "posting_number": posting_number,
                    "article": p.get("offer_id") or "",
                    "name": p.get("name") or "",
                    "reason": reason,
                }
            )
    else:
        rows.append(
            {
                "posting_number": posting_number,
                "article": "",
                "name": "",
                "reason": reason,
            }
        )

    return rows

def build_ms_positions_from_posting(posting: dict) -> list[dict]:
    products = posting.get("products") or []
    ms_positions = []
    missing = []

    for p in products:
        offer_id = p.get("offer_id")
        qty = p.get("quantity", 0)
        if not offer_id or qty <= 0:
            continue

        ms_product = find_product_by_article(offer_id)
        if not ms_product:
            missing.append(offer_id)
            continue

        ms_positions.append(
            {
                "quantity": qty,
                "ms_meta": ms_product["meta"],
                "article": offer_id,
            }
        )

    if missing:
        text = (
            "❗ Не найдены товары в МойСклад по артикулу из Ozon\n"
            f"Отправление: {posting.get('posting_number')}\n"
            f"Артикулы: {', '.join(missing)}"
        )
        print("[ORDERS]", text.replace("\n", " | "))
        return []

    return ms_positions

async def send_report_to_telegram(file_path):
    """Функция для отправки файла в Telegram асинхронно."""
    bot = Bot(token=os.getenv('TG_BOT_TOKEN'))
    chat_id = os.getenv('TG_CHAT_ID')
    with open(file_path, "rb") as f:
        await bot.send_document(chat_id=chat_id, document=f)

def sync_fbs_orders(dry_run: bool, limit: int = 300):
    print(f"[ORDERS] Старт sync_fbs_orders, DRY_RUN_ORDERS={dry_run}")

    postings: list[dict] = []

    # --- Кабинет Ozon №1 (как раньше) ---
    data1 = get_fbs_postings_ozon1(limit=limit)
    p1 = data1.get("result", {}).get("postings", []) or []
    for p in p1:
        # помечаем источник, если вдруг потом пригодится
        p.setdefault("_ozon_account", "ozon1")
    postings.extend(p1)

    # --- Кабинет Ozon №2 (если настроен и импорт прошёл) ---
    if get_fbs_postings_ozon2 is not None:
        try:
            data2 = get_fbs_postings_ozon2(limit=limit)
            p2 = data2.get("result", {}).get("postings", []) or []
            for p in p2:
                p.setdefault("_ozon_account", "ozon2")
            postings.extend(p2)
        except Exception as e:
            msg = f"❗ Ошибка при получении отправлений из второго кабинета Ozon: {e!r}"
            print("[ORDERS]", msg)
            try:
                send_telegram_message(msg)
            except Exception:
                pass

    print(f"[ORDERS] Найдено отправлений: {len(postings)}")

    # Определяем дату, до которой заказы должны быть игнорированы
    cutoff_date = datetime(2025, 11, 30)

    error_rows: list[dict] = []

    for posting in postings:
        # Получаем дату создания заказа
        created_date_str = posting.get("created")
        created_date = datetime.strptime(created_date_str, "%Y-%m-%d") if created_date_str else None

        # Пропускаем заказ, если он был создан до 30.11.2025
        if created_date and created_date < cutoff_date:
            print(
                f"[ORDERS] Заказ {posting.get('posting_number')} "
                f"(источник={posting.get('_ozon_account')}) создан до 30.11.2025, пропускаем."
            )
            continue  # Пропускаем этот заказ

        try:
            # существующая логика обработки одного отправления
            process_posting(posting, dry_run)
        except Exception as e:
            reason = _human_error_from_exception(e)
            error_rows.extend(_build_error_rows_for_posting(posting, reason))

    # После обработки заказов — отправляем файл с ошибками в Telegram
    _append_order_errors_to_file(error_rows)
    asyncio.run(send_report_to_telegram(ERRORS_FILE_PATH))

if __name__ == "__main__":
    print("Запуск синхронизации заказов Ozon с МойСклад...")
    sync_fbs_orders(dry_run=DRY_RUN_ORDERS, limit=300)
