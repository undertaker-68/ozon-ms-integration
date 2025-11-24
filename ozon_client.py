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
    """
    if not offer_ids:
        return {}

    url = f"{OZON_API_URL}/v3/product/info/list"

    BATCH_SIZE = 1000
    result = {}

    for i in range(0, len(offer_ids), BATCH_SIZE):
        batch = offer_ids[i:i + BATCH_SIZE]

        body = {
            "offer_id": batch,
            "product_id": [],
            "sku": [],
        }

        print("=== Тело запроса к Ozon /v3/product/info/list ===")
        print(body)
        print("=== /Тело запроса ===\n")

        r = requests.post(url, json=body, headers=HEADERS, timeout=30)

        print("=== Ответ Ozon /v3/product/info/list ===")
        print("HTTP status:", r.status_code)
        try:
            data = r.json()
            print("JSON (фрагмент):", str(data)[:500])
        except Exception:
            data = {}
            print("TEXT:", r.text[:500])
        print("=== /Ответ Ozon ===\n")

        r.raise_for_status()

        items = data.get("result") or data.get("items") or []

        for item in items:
            oid = item.get("offer_id")
            state = item.get("state")
            if oid:
                result[oid] = state

    return result


def update_stocks(stocks: list) -> dict:
    """
    Обновление остатков в Ozon.

    Ожидает список словарей вида:
    {
        "offer_id": "ART123",
        "stock": 10,
        "warehouse_id": 22254230484000
    }
    """
    if not stocks:
        print("update_stocks: передан пустой список stocks, запрос к Ozon не отправляется.")
        return {"result": []}

    url = f"{OZON_API_URL}/v2/products/stocks"
    body = {"stocks": stocks}

    print("=== Тело запроса к Ozon /v2/products/stocks ===")
    print(json.dumps(body, ensure_ascii=False, indent=2))
    print("=== /Тело запроса ===\n")

    try:
        r = requests.post(url, json=body, headers=HEADERS, timeout=30)
    except Exception as e:
        msg = f"❗ Ошибка запроса к Ozon /v2/products/stocks:\n{e!r}"
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
        msg = (
            "❗ Ozon вернул ошибку при обновлении остатков\n"
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
        msg = (
            "❗ Ozon /v2/products/stocks вернул не-JSON ответ\n"
            f"HTTP status: {r.status_code}\n"
            f"Response: {text_fragment}"
        )
        try:
            send_telegram_message(msg)
        except Exception:
            pass
        raise RuntimeError("Некорректный JSON от Ozon при обновлении остатков")

    result_items = data.get("result", [])
    errors_summary = []

    for item in result_items:
        offer_id = item.get("offer_id")
        updated = item.get("updated", False)
        errors = item.get("errors") or []

        if errors:
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
            send_telegram_message("⚠ Ошибки при обновлении остатков в Ozon:\n\n" + "\n\n".join(errors_summary))
        except Exception:
            pass

    return data


def get_fbs_postings(limit: int = 3) -> dict:
    """
    Получение FBS-отправлений Ozon за последние 7 дней.
    Использует /v3/posting/fbs/list.
    """
    url = f"{OZON_API_URL}/v3/posting/fbs/list"

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=7)

    body = {
        "dir": "ASC",
        "filter": {
            "since": since.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "to": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
            # Ozon требует хотя бы один статус
            "status": [
                "awaiting_packaging",
                "awaiting_deliver",
                "delivering",
                "cancelled",
                "delivered",
            ],
        },
        "limit": limit,
        "offset": 0,
        "with": {
            "analytics_data": True,
            "financial_data": True,
        },
    }

    print("=== Тело запроса к Ozon /v3/posting/fbs/list ===")
    print(json.dumps(body, ensure_ascii=False, indent=2))
    print("=== /Тело запроса ===\n")

    r = requests.post(url, json=body, headers=HEADERS, timeout=30)

    print("=== Ответ Ozon /v3/posting/fbs/list ===")
    print("HTTP status:", r.status_code)
    print("Response text:")
    print(r.text[:5000])
    print("=== /Ответ Ozon ===\n")

    r.raise_for_status()
    return r.json()


if __name__ == "__main__":
    print("Тест запроса FBS-отправлений...")
    data = get_fbs_postings(limit=1)
    print("OK, получен ответ от Ozon.")
