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
    from notifier import send_telegram_message, send_telegram_document
except ImportError:
    def send_telegram_message(text: str) -> bool:
        print("Telegram notifier не доступен:", text)
        return False

    def send_telegram_document(file_path: str, caption: str = "") -> bool:
        print(f"Telegram notifier не доступен для отправки файла: {file_path} ({caption})")
        return False

try:
    from ozon_client2 import get_fbs_postings as get_fbs_postings_ozon2
except ImportError:
    get_fbs_postings_ozon2 = None

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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ERRORS_AUTO_FILE_PATH = os.path.join(
    BASE_DIR,
    "orders_errors_auto.csv",
)

ERRORS_TRAIL_FILE_PATH = os.path.join(
    BASE_DIR,
    "orders_errors_trail.csv",
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

def _append_order_errors_to_file(file_path: str, rows: list[dict]) -> None:
    """
    Записываем ошибки обработки заказов в CSV.
    Колонки: дата/время, номер заказа, артикул, название, причина ошибки.
    """
    if not rows:
        return

    file_exists = os.path.exists(file_path)

    with open(file_path, "a", newline="", encoding="utf-8-sig") as f:
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

def process_posting(posting: dict, dry_run: bool) -> None:
    """
    Обработка одного FBS-отправления (оба кабинета):
      - создаём/обновляем заказ в МойСклад
      - в комментарий заказа пишем:
          'FBS → Auto-MiX' для первого кабинета
          'FBS → Trail Gear' для второго кабинета
      - при статусах delivering/delivered создаём отгрузку
    """
    posting_number = posting.get("posting_number")
    status = posting.get("status")
    ozon_account = posting.get("_ozon_account") or "ozon1"

    # Номер заказа в МС = номеру в Ozon (БЕЗ префиксов)
    order_name = posting_number or "UNKNOWN"

    # Карта статусов
    status_map = {
        "awaiting_packaging": MS_STATE_AWAIT_PACK,
        "awaiting_deliver": MS_STATE_AWAIT_SHIP,
        "delivering": MS_STATE_DELIVERING,
        "delivered": MS_STATE_DELIVERED,
        "cancelled": MS_STATE_CANCELLED,
    }
    state_meta_href = status_map.get(status)

    # Позиции МойСклад
    ms_positions = build_ms_positions_from_posting(posting)
    if not ms_positions:
        raise RuntimeError("Не удалось сопоставить ни одной позиции с товарами МойСклад")

    positions_payload = [
        {
            "quantity": pos["quantity"],
            "assortment": {"meta": pos["ms_meta"]},
        }
        for pos in ms_positions
    ]

    org_meta = {
        "href": MS_ORGANIZATION_HREF,
        "type": "organization",
        "mediaType": "application/json",
    }
    agent_meta = {
        "href": MS_AGENT_HREF,
        "type": "counterparty",
        "mediaType": "application/json",
    }
    store_meta = {
        "href": MS_STORE_HREF,
        "type": "store",
        "mediaType": "application/json",
    }

    # Комментарий в заказе
    if ozon_account in ("ozon2", "trail_gear"):
        description = "FBS → Trail Gear"
    else:
        description = "FBS → Auto-MiX"

    payload = {
        "name": order_name,
        "organization": {"meta": org_meta},
        "agent": {"meta": agent_meta},
        "store": {"meta": store_meta},
        "positions": positions_payload,
        "description": description,
    }

    if state_meta_href:
        payload["state"] = {
            "meta": {
                "href": state_meta_href,
                "type": "state",
                "mediaType": "application/json",
            }
        }

    print(
        f"[ORDERS] Обработка отправления {posting_number} "
        f"(аккаунт={ozon_account}, статус={status}), "
        f"позиций: {len(positions_payload)}, DRY_RUN={dry_run}"
    )

    if dry_run:
        return

    # Проверяем, есть ли уже такой заказ
    existing = find_customer_order_by_name(order_name)
    if existing:
        print(f"[ORDERS] Заказ {order_name} уже существует в МойСклад.")
        # обновляем статус (если есть)
        if state_meta_href:
            update_customer_order_state(existing["meta"]["href"], state_meta_href)

        # При delivering/delivered создаём отгрузку и для существующего заказа
                if status in ("delivering", "delivered"):
            try:
                create_demand_from_order(existing["meta"]["href"])
            except Exception as e:
                msg = (
                    f"[ORDERS] Ошибка создания отгрузки для существующего заказа "
                    f"{order_name}: {e!r}"
                )
                print(msg)
                try:
                    send_telegram_message(msg)
                except Exception:
                    pass
                raise

    # Создаём новый заказ
    created = create_customer_order(payload)

    # Если заказ уже в доставке/доставлен — сразу делаем отгрузку
    if status in ("delivering", "delivered"):
        try:
            create_demand_from_order(created["meta"]["href"])
                except Exception as e:
            msg = f"[ORDERS] Ошибка создания отгрузки для заказа {order_name}: {e!r}"
            print(msg)
            try:
                send_telegram_message(msg)
            except Exception:
                pass
            # пробрасываем исключение наверх, чтобы sync_fbs_orders добавил его в CSV
            raise

async def send_report_to_telegram(file_path):
    """Функция для отправки файла в Telegram асинхронно."""
    bot = Bot(token=os.getenv('TG_BOT_TOKEN'))
    chat_id = os.getenv('TG_CHAT_ID')
    with open(file_path, "rb") as f:
        await bot.send_document(chat_id=chat_id, document=f)

def sync_fbs_orders(dry_run: bool, limit: int = 300):
    print(f"[ORDERS] Старт sync_fbs_orders, DRY_RUN_ORDERS={dry_run}")

    # Уведомление о запуске из cron (или любого другого запуска)
    try:
        send_telegram_message(
            f"🔁 CRON: запуск sync_fbs_orders (заказы), DRY_RUN_ORDERS={dry_run}"
        )
    except Exception as e:
        print("[ORDERS] Не удалось отправить телеграм-уведомление о запуске:", e)

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
    # Обрабатываем только заказы, созданные 01.12.2025 и позже
    cutoff_date = datetime(2025, 12, 1)

    errors_auto: list[dict] = []
    errors_trail: list[dict] = []

    for posting in postings:
            # Получаем дату создания заказа из поля created_at (Ozon FBS API)
    created_date_str = posting.get("created_at") or posting.get("created") or posting.get("in_process_at")
    created_date = None

    if created_date_str:
        # Берём только дату YYYY-MM-DD, игнорируя время и часовой пояс
        try:
            created_date = datetime.strptime(created_date_str[:10], "%Y-%m-%d")
        except Exception:
            created_date = None

        # Пропускаем заказ, если он был создан до 01.12.2025
        if created_date and created_date < cutoff_date:
            print(
                f"[ORDERS] Заказ {posting.get('posting_number')} "
                f"(источник={posting.get('_ozon_account')}) создан до 01.12.2025, пропускаем."
            )
            continue  # Пропускаем этот заказ

        try:
            process_posting(posting, dry_run)
        except Exception as e:
            reason = _human_error_from_exception(e)
            rows = _build_error_rows_for_posting(posting, reason)
            acc = posting.get("_ozon_account")
            if acc in ("ozon2", "trail_gear"):
                errors_trail.extend(rows)
            else:
                errors_auto.extend(rows)

    # После обработки заказов — пишем CSV и отправляем ДВА файла с ошибками
    _append_order_errors_to_file(ERRORS_AUTO_FILE_PATH, errors_auto)
    _append_order_errors_to_file(ERRORS_TRAIL_FILE_PATH, errors_trail)

    if errors_auto:
        send_telegram_document(ERRORS_AUTO_FILE_PATH, caption="Ошибки Auto-MiX")
    if errors_trail:
        send_telegram_document(ERRORS_TRAIL_FILE_PATH, caption="Ошибки Trail Gear")

if __name__ == "__main__":
    print("Запуск синхронизации заказов Ozon с МойСклад...")
    sync_fbs_orders(dry_run=DRY_RUN_ORDERS, limit=300)
