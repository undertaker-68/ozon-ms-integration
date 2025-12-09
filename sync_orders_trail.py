import os
import csv
from datetime import datetime
import requests
import asyncio
from dotenv import load_dotenv

from ozon_client2 import get_fbs_postings
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

# Общий CSV для ошибок (как у первого кабинета)
ERRORS_FILE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "orders_errors.csv",
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
    """
    Собираем позиции для заказа МойСклад по товарам из отправления Ozon.
    Если какие-то товары не найдены — логируем и даём вызвать обработку ошибки выше.
    """
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
            "❗ Не найдены товары в МойСклад по артикулу из Ozon (Trail Gear)\n"
            f"Отправление: {posting.get('posting_number')}\n"
            f"Артикулы: {', '.join(missing)}"
        )
        print("[ORDERS TG]", text.replace("\n", " | "))

        # как в первом кабинете — не создаём заказ по этому отправлению
        return []

    return ms_positions


async def send_report_to_telegram(file_path: str):
    """Отправка файла с ошибками в Telegram асинхронно (если понадобится вызывать вручную)."""
    bot = Bot(token=os.getenv("TG_BOT_TOKEN"))
    chat_id = os.getenv("TG_CHAT_ID")
    if not chat_id:
        print("[ORDERS TG] TG_CHAT_ID не задан, отчет не отправлен.")
        return

    with open(file_path, "rb") as f:
        await bot.send_document(chat_id=chat_id, document=f, caption="Ошибки Trail Gear")


def process_posting(posting: dict, dry_run: bool) -> None:
    """
    Обработка одного FBS-отправления Trail Gear:
      - создаём/обновляем заказ в МойСклад
      - в комментарий заказа пишем: 'FBS → Trail Gear'
    """
    posting_number = posting.get("posting_number")
    status = posting.get("status")
    ozon_account = posting.get("_ozon_account", "trail_gear")

    order_name = f"TG-{posting_number}" if posting_number else "TG-UNKNOWN"

    # Подбираем соответствующий статус в МойСклад (если есть)
    status_map = {
        "awaiting_packaging": MS_STATE_AWAIT_PACK,
        "awaiting_deliver": MS_STATE_AWAIT_SHIP,
        "delivering": MS_STATE_DELIVERING,
        "delivered": MS_STATE_DELIVERED,
        "cancelled": MS_STATE_CANCELLED,
    }
    state_meta_href = status_map.get(status)

    ms_positions = build_ms_positions_from_posting(posting)
    if not ms_positions:
        raise RuntimeError("Не удалось сопоставить ни одной позиции с товарами МойСклад")

        positions_payload = [
        {
            "quantity": pos["quantity"],
            "assortment": {
                "meta": pos["ms_meta"],
            },
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

    payload = {
        "name": order_name,
        "organization": {"meta": org_meta},
        "agent": {"meta": agent_meta},
        "store": {"meta": store_meta},
        "positions": positions_payload,
        "description": "FBS → Trail Gear",
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
        f"[ORDERS TG] Обработка отправления {posting_number} "
        f"(аккаунт={ozon_account}, статус={status}), "
        f"позиций: {len(positions_payload)}, DRY_RUN={dry_run}"
    )

    if dry_run:
        return

    # Проверяем, есть ли уже такой заказ
    existing = find_customer_order_by_name(order_name)
    if existing:
        print(f"[ORDERS TG] Заказ {order_name} уже существует в МойСклад.")
        # при необходимости можем обновить статус
        if state_meta_href:
            update_customer_order_state(existing["meta"]["href"], state_meta_href)
        return

        # Создаём новый заказ
    created = create_customer_order(payload)

    # Если заказ уже в стадии доставки/доставлен — можно сразу сделать отгрузку
    if status in ("delivering", "delivered"):
        try:
            # БЫЛО: create_demand_from_order(created["meta"]["href"])
            create_demand_from_order(created)
        except Exception as e:
            msg = (
                f"[ORDERS TG] Ошибка создания отгрузки для заказа {order_name}: {e!r}"
            )
            print(msg)
            try:
                send_telegram_message(msg)
            except Exception:
                pass

def sync_fbs_orders(dry_run: bool, limit: int = 300):
    print(f"[ORDERS TG] Старт sync_fbs_orders (Trail Gear), DRY_RUN_ORDERS={dry_run}")

    data = get_fbs_postings(limit=limit)
    postings = data.get("result", {}).get("postings", []) or []

    print(f"[ORDERS TG] Найдено отправлений: {len(postings)}")

    # Ограничение по дате
    cutoff_date = datetime(2025, 12, 2)

    error_rows: list[dict] = []

    for posting in postings:
        created_date_str = posting.get("created")
        created_date = None

        if created_date_str:
            # безопасно отрезаем только дату
            try:
                created_date = datetime.strptime(created_date_str[:10], "%Y-%m-%d")
            except Exception:
                created_date = None

        if created_date and created_date <= cutoff_date:
            print(
                f"[ORDERS TG] Заказ {posting.get('posting_number')}"
                f"создан ≤ 02.12.2025, пропускаем."
            )
            continue

        try:
            process_posting(posting, dry_run)
        except Exception as e:
            reason = _human_error_from_exception(e)
            error_rows.extend(_build_error_rows_for_posting(posting, reason))

    _append_order_errors_to_file(error_rows)


if __name__ == "__main__":
    print("Запуск синхронизации заказов Ozon (Trail Gear) с МойСклад...")
    sync_fbs_orders(dry_run=DRY_RUN_ORDERS, limit=300)
