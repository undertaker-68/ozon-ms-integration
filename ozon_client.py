import os
import json
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

CLIENT_ID = os.getenv("OZON_CLIENT_ID")
API_KEY = os.getenv("OZON_API_KEY")

if not CLIENT_ID or not API_KEY:
    raise RuntimeError("Не заданы OZON_CLIENT_ID / OZON_API_KEY в .env")

HEADERS = {
    "Client-Id": CLIENT_ID,
    "Api-Key": API_KEY,
    "Content-Type": "application/json",
}

OZON_API_URL = "https://api-seller.ozon.ru"


def get_products_state_by_offer_ids(offer_ids):
    """
    Возвращает словарь {offer_id: state} для переданных offer_id.
    Использует /v3/product/info/list.
      - ARCHIVED, если is_archived или is_autoarchived = True
      - ACTIVE  иначе
      - None   если товар вообще не найден Ozon
    """
    if not offer_ids:
        return {}

    url = f"{OZON_API_URL}/v3/product/info/list"

    BATCH_SIZE = 1000
    result = {}

    for i in range(0, len(offer_ids), BATCH_SIZE):
        batch = offer_ids[i:i + BATCH_SIZE]
        body = {"offer_id": batch, "product_id": [], "sku": []}

        r = requests.post(url, json=body, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            msg = (
                f"❗ Ошибка Ozon /v3/product/info/list: status={r.status_code}, "
                f"body={r.text[:500]}"
            )
            print(msg)
            try:
                send_telegram_message(msg)
            except Exception:
                pass
            r.raise_for_status()

        data = r.json()
        items = data.get("items") or data.get("result") or []

        for item in items:
            oid = item.get("offer_id")
            if not oid:
                continue
            is_archived = bool(item.get("is_archived")) or bool(item.get("is_autoarchived"))
            state = "ARCHIVED" if is_archived else "ACTIVE"
            result[oid] = state

    for oid in offer_ids:
        if oid not in result:
            result[oid] = None

    return result


def update_stocks(stocks: list) -> dict:
    """
    Обновление остатков в Ozon.

    Принимает список словарей вида:
    {
        "offer_id": "ART123",
        "stock": 10,
        "warehouse_id": 22254230484000
    }

    Ozon ограничивает размер массива stocks: 1..100 элементов.
    Поэтому отправляем батчами по 100.
    """
    if not stocks:
        print("update_stocks: передан пустой список stocks, запрос к Ozon не отправляется.")
        return {"result": []}

    url = f"{OZON_API_URL}/v2/products/stocks"
    BATCH_SIZE = 100

    all_results = []
    any_errors = False

    print(f"[OZON] Обновление остатков, позиций: {len(stocks)}")

    for i in range(0, len(stocks), BATCH_SIZE):
        batch = stocks[i:i + BATCH_SIZE]
        body = {"stocks": batch}
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(stocks) + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"[OZON] Отправка батча {batch_num}/{total_batches}, позиций: {len(batch)}")
        print("=== Тело запроса к Ozon /v2/products/stocks ===")
        print(json.dumps(body, ensure_ascii=False, indent=2))
        print("=== /Тело запроса ===\n")

        try:
            r = requests.post(url, json=body, headers=HEADERS, timeout=30)
        except Exception as e:
            msg = f"❗ Ошибка запроса к Ozon /v2/products/stocks (батч {batch_num}/{total_batches}):\n{e!r}"
            print(msg)
            try:
                send_telegram_message(msg)
            except Exception:
                pass
            raise

        print("=== Ответ Ozon /v2/products/stocks ===")
        print("HTTP status:", r.status_code)
        text_fragment = r.text[:2000]
        print(text_fragment)
        print("=== /Ответ Ozon ===\n")

        if r.status_code != 200:
            any_errors = True
            msg = (
                "❗ Ozon вернул ошибку при обновлении остатков\n"
                f"Батч: {batch_num}/{total_batches}\n"
                f"HTTP status: {r.status_code}\n"
                f"Response: {text_fragment}"
            )
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
                "❗ Ozon /v2/products/stocks вернул не-JSON ответ\n"
                f"Батч: {batch_num}/{total_batches}\n"
                f"HTTP status: {r.status_code}\n"
                f"Response: {text_fragment}"
            )
            try:
                send_telegram_message(msg)
            except Exception:
                pass
            raise RuntimeError("Некорректный JSON от Ozon при обновлении остатков")

        result_items = data.get("result", [])
        all_results.extend(result_items)

        errors_summary = []
        for item in result_items:
            offer_id = item.get("offer_id")
            errors = item.get("errors") or []
            if errors:
                any_errors = True
                for err in errors:
                    code = err.get("code")
                    message = err.get("message")
                    msg = (
                        "❗ Ошибка обновления остатка в Ozon по товару\n"
                        f"offer_id: {offer_id}\n"
                        f"code: {code}\n"
                        f"message: {message}"
                    )
                    print(msg)
                    errors_summary.append(msg)

        if errors_summary:
            try:
                send_telegram_message(
                    "⚠ Ошибки при обновлении остатков в Ozon "
                    f"(батч {batch_num}/{total_batches}):\n\n" + "\n\n".join(errors_summary)
                )
            except Exception:
                pass

    if not any_errors:
        print(f"[OZON] Обновление остатков завершено, ошибок нет. Всего позиций: {len(all_results)}")
    else:
        print("[OZON] Обновление остатков завершено с ошибками, подробности выше/в Telegram.")

    return {"result": all_results}


def get_fbs_postings(limit: int = 3) -> dict:
    """
    Получение FBS-отправлений Ozon за последние 7 дней.
    Использует /v3/posting/fbs/list.
    filter.status — ОДНА строка, вызываем по каждому статусу и собираем до limit штук.
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

    all_postings = []
    seen_numbers = set()

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
                "❗ Ошибка Ozon /v3/posting/fbs/list\n"
                f"status={r.status_code}, body={r.text[:500]}"
            )
            print(msg)
            try:
                send_telegram_message(msg)
            except Exception:
                pass
            r.raise_for_status()

        data = r.json()
        postings = data.get("result", {}).get("postings", [])

        for p in postings:
            pn = p.get("posting_number")
            if not pn or pn in seen_numbers:
                continue
            seen_numbers.add(pn)
            all_postings.append(p)
            if len(all_postings) >= limit:
                break

    print(f"[OZON] Получено FBS-отправлений: {len(all_postings)}")
    return {"result": {"postings": all_postings}}
