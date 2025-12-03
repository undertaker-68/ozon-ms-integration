import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

try:
    from notifier import send_telegram_message
except ImportError:
    def send_telegram_message(text: str) -> bool:
        print("Telegram notifier не доступен:", text)
        return False


load_dotenv()

CLIENT_ID = os.getenv("OZON2_CLIENT_ID")
API_KEY = os.getenv("OZON2_API_KEY")

if not CLIENT_ID or not API_KEY:
    raise RuntimeError("Не заданы OZON2_CLIENT_ID / OZON2_API_KEY в .env")

HEADERS = {
    "Client-Id": CLIENT_ID,
    "Api-Key": API_KEY,
    "Content-Type": "application/json",
}

OZON_API_URL = "https://api-seller.ozon.ru"


def get_products_state_by_offer_ids(offer_ids):
    """
    Возвращает словарь {offer_id: state} для переданных offer_id (2-й кабинет).
    Использует /v3/product/info/list.

    Логика такая же, как в первом кабинете:
      - "ARCHIVED", если:
          * is_archived или is_autoarchived = True
          * ИЛИ state/status = archived/disabled
      - "ACTIVE"  иначе
      - None      если товар вообще не найден
    """
    if not offer_ids:
        return {}

    url = f"{OZON_API_URL}/v3/product/info/list"
    BATCH_SIZE = 1000
    result: dict[str, str | None] = {}

    for i in range(0, len(offer_ids), BATCH_SIZE):
        batch = offer_ids[i:i + BATCH_SIZE]
        body = {"offer_id": batch}

        r = requests.post(url, json=body, headers=HEADERS, timeout=30)

        if r.status_code != 200:
            msg = (
                "❗ Ошибка Ozon2 /v3/product/info/list\n"
                f"HTTP {r.status_code}\n"
                f"{r.text[:2000]}"
            )
            print(msg)
            try:
                send_telegram_message(msg)
            except Exception:
                pass
            r.raise_for_status()

        try:
            data = r.json()
        except Exception:
            print("❗ Ошибка парсинга JSON Ozon2 /v3/product/info/list:", r.text[:500])
            continue

        items = data.get("items") or data.get("result") or []

        for item in items:
            oid = item.get("offer_id")
            if not oid:
                continue

            # флаги архива
            is_archived_flag = bool(item.get("is_archived")) or bool(item.get("is_autoarchived"))

            # текстовые статусы
            state_raw = (item.get("state") or item.get("status") or "").strip()
            state_upper = state_raw.upper()

            if is_archived_flag or state_upper in ("ARCHIVED", "DISABLED"):
                state = "ARCHIVED"
            else:
                state = "ACTIVE"

            result[oid] = state

    # Для всех, кто не пришёл в ответе, явно проставим None
    for oid in offer_ids:
        if oid not in result:
            result[oid] = None

    return result


def update_stocks(stocks: list) -> dict:
    """
    Обновление остатков в Ozon (кабинет Trail Gear).

    Формат элементов:
    {
        "offer_id": "ART123",
        "stock": 10,
        "warehouse_id": 22254230484000
    }

    Отправляем батчами по 100 позиций.
    Ошибки:
      - в логи выводим все
      - в Telegram:
          * реальные ошибки (кроме TOO_MANY_REQUESTS) — только первые N строк
          * TOO_MANY_REQUESTS — короткое резюме, без простыни
    """
    if not stocks:
        print("[OZON2] update_stocks: передан пустой список stocks, запрос к Ozon2 не отправляется.")
        return {"result": []}

    url = f"{OZON_API_URL}/v2/products/stocks"

    any_errors = False
    all_results: list[dict] = []

    batch_size = 100
    total = len(stocks)
    total_batches = (total + batch_size - 1) // batch_size

    for batch_num in range(1, total_batches + 1):
        start = (batch_num - 1) * batch_size
        end = min(start + batch_size, total)
        batch = stocks[start:end]

        body = {"stocks": batch}

        print(f"[OZON2] Обновление остатков, батч {batch_num}/{total_batches}, позиций: {len(batch)}")
        r = requests.post(url, json=body, headers=HEADERS, timeout=30)

        text_fragment = r.text[:500]

        if r.status_code != 200:
            any_errors = True
            msg = (
                "❗ Ошибка Ozon2 /v2/products/stocks\n"
                f"Батч: {batch_num}/{total_batches}\n"
                f"HTTP status: {r.status_code}\n"
                f"Response: {text_fragment}"
            )
            print(msg)
            try:
                send_telegram_message(msg)
            except Exception:
                pass
            r.raise_for_status()

        try:
            data = r.json()
        except Exception:
            any_errors = True
            msg = (
                "❗ Ozon2 /v2/products/stocks вернул не-JSON ответ\n"
                f"Батч: {batch_num}/{total_batches}\n"
                f"HTTP status: {r.status_code}\n"
                f"Response: {text_fragment}"
            )
            print(msg)
            try:
                send_telegram_message(msg)
            except Exception:
                pass
            raise RuntimeError("Некорректный JSON от Ozon2 при обновлении остатков")

        result_items = data.get("result", []) or []
        all_results.extend(result_items)

        # Собираем ошибки: отдельно считаем TOO_MANY_REQUESTS,
        # отдельно — остальные (для краткого отчёта)
        errors_summary: list[str] = []
        too_many_requests_count = 0

        for item in result_items:
            offer_id = item.get("offer_id")
            errors = item.get("errors") or []
            if not errors:
                continue

            any_errors = True

            for err in errors:
                code = err.get("code")
                message = err.get("message")

                msg = (
                    "❗ Ошибка обновления остатка в Ozon2 по товару\n"
                    f"offer_id: {offer_id}\n"
                    f"code: {code}\n"
                    f"message: {message}"
                )
                # Полный текст — в логи
                print(msg)

                if code == "TOO_MANY_REQUESTS":
                    too_many_requests_count += 1
                else:
                    errors_summary.append(msg)

        # Краткое сообщение по "настоящим" ошибкам (кроме TOO_MANY_REQUESTS)
        if errors_summary:
            MAX_LINES = 20  # чтобы сообщение не стало слишком длинным
            trimmed = errors_summary[:MAX_LINES]

            text = (
                f"⚠ Ошибки при обновлении остатков в Ozon2 "
                f"(батч {batch_num}/{total_batches}). "
                f"Показываю первые {len(trimmed)} из {len(errors_summary)}:\n\n"
                + "\n\n".join(trimmed)
            )

            try:
                send_telegram_message(text)
            except Exception as te:
                print("Ошибка отправки уведомления в Telegram (Ozon2 errors):", te)

        # Отдельное короткое сообщение по TOO_MANY_REQUESTS
        if too_many_requests_count:
            try:
                send_telegram_message(
                    f"⚠ Ozon2 вернул TOO_MANY_REQUESTS при обновлении остатков: "
                    f"{too_many_requests_count} позиций (батч {batch_num}/{total_batches}). "
                    f"Обычно это значит, что остатки обновляются слишком часто."
                )
            except Exception as te:
                print("Ошибка отправки уведомления в Telegram (TOO_MANY_REQUESTS):", te)

    if not any_errors:
        print(f"[OZON2] Обновление остатков завершено, ошибок нет. Всего позиций: {len(all_results)}")
    else:
        print("[OZON2] Обновление остатков завершено с ошибками, подробности см. выше и в Telegram.")

    return {"result": all_results}


def get_fbs_postings(limit: int = 3) -> dict:
    """
    Получение FBS-отправлений из ВТОРОГО кабинета Ozon (Trail Gear)
    за последние 7 дней. Логика аналогична основному ozon_client.get_fbs_postings.
    """
    url = f"{OZON_API_URL}/v3/posting/fbs/list"

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=7)

    STATUSES = [
        "awaiting_packaging",
        "awaiting_deliver",
        "delivering",
        "cancelled",
        "delivered",
    ]

    all_postings: list[dict] = []
    seen_numbers: set[str] = set()

    for status in STATUSES:
        if len(all_postings) >= limit:
            break

        body = {
            "dir": "ASC",
            "filter": {
                "since": since.isoformat(timespec="seconds").replace("+00:00", "Z"),
                "to": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
                "status": status,
                "fbp_filter": "ALL",
            },
            "limit": limit,
            "offset": 0,
            "with": {
                "analytics_data": True,
                "financial_data": True,
            },
        }

        r = requests.post(url, json=body, headers=HEADERS, timeout=30)

        if r.status_code != 200:
            msg = (
                "❗ Ошибка Ozon2 /v3/posting/fbs/list\n"
                f"status={r.status_code}, body={r.text[:500]}"
            )
            print(msg)
            try:
                send_telegram_message(msg)
            except Exception:
                pass
            r.raise_for_status()

        data = r.json()
        postings = data.get("result", {}).get("postings", []) or []

        for p in postings:
            pn = p.get("posting_number")
            if not pn or pn in seen_numbers:
                continue
            seen_numbers.add(pn)

            # Помечаем, что это Trail Gear
            p["_ozon_account"] = "trail_gear"

            all_postings.append(p)
            if len(all_postings) >= limit:
                break

    print(f"[OZON2] Получено FBS-отправлений (Trail Gear): {len(all_postings)}")
    return {"result": {"postings": all_postings}}
