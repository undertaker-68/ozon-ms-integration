import os
import json
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

try:
    # наш модуль для уведомлений
    from notifier import send_telegram_message
except ImportError:
    # запасной вариант, чтобы файл не падал, если notifier нет
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
BASE_URL = OZON_API_URL


def get_products_state_by_offer_ids(offer_ids: list[str]) -> dict:
    """
    Получает состояние товаров Ozon через актуальный метод v3/products/info
    """
    if not offer_ids:
        return {}

    url = f"{OZON_API_URL}/v3/products/info"

    # Ozon позволяет до 1000 offer_id за раз
    BATCH_SIZE = 1000
    result: dict[str, str | None] = {}

    for i in range(0, len(offer_ids), BATCH_SIZE):
        batch = offer_ids[i:i + BATCH_SIZE]

        body = {
            "offer_id": batch,
            "product_id": [],
            "sku": []
        }

        r = requests.post(url, json=body, headers=HEADERS, timeout=30)

        if r.status_code != 200:
            print("Ошибка запроса /v3/products/info:", r.text)
            r.raise_for_status()

        try:
            data = r.json()
        except:
            print("Ошибка JSON при /v3/products/info:", r.text[:500])
            raise

        items = data.get("result", [])
        for item in items:
            oid = item.get("offer_id")
            state = item.get("state")
            if oid:
                result[oid] = state

        # те offer_id, которые не вернулись — считаем отсутствующими
        for missing in batch:
            if missing not in result:
                result[missing] = None

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
        msg = f"❗ Ошибка сети при обновлении остатков в Ozon: {e!r}"
        print(msg)
        try:
            send_telegram_message(msg)
        except Exception:
            pass
        # пробрасываем выше, чтобы скрипт знал, что всё плохо
        raise

    print("=== Ответ Ozon /v2/products/stocks ===")
    print("HTTP status:", r.status_code)
    raw_text = r.text
    print(raw_text[:1000])  # чтобы не захламлять лог
    print("=== /Ответ Ozon ===\n")

    # Если HTTP-код не 200 — шлём уведомление и падаем
    if r.status_code != 200:
        msg = (
            f"❗ Ошибка обновления остатков в Ozon (HTTP {r.status_code}):\n"
            f"{raw_text[:3000]}"
        )
        try:
            send_telegram_message(msg)
        except Exception:
            pass
        r.raise_for_status()

    # Пытаемся разобрать JSON
    try:
        data = r.json()
    except ValueError:
        msg = (
            "❗ Ozon вернул не-JSON при обновлении остатков.\n"
            f"Фрагмент ответа:\n{raw_text[:3000]}"
        )
        print(msg)
        try:
            send_telegram_message(msg)
        except Exception:
            pass
        raise

    # Пытаемся вытащить ошибки по отдельным товарам, если Ozon их даёт
    errors = []

    if isinstance(data, dict):
        # Часто в ответах есть поля errors / failed
        if isinstance(data.get("errors"), list):
            errors.extend(data["errors"])
        if isinstance(data.get("failed"), list):
            errors.extend(data["failed"])

        # На всякий случай проверим result как массив
        if isinstance(data.get("result"), list):
            for item in data["result"]:
                if not isinstance(item, dict):
                    continue
                # очень общая эвристика: если явно указано, что операция неуспешна
                if item.get("success") is False or item.get("result") is False:
                    errors.append(item)

    if errors:
        # Шлём только усечённый фрагмент, чтобы не словить лимиты по длине
        try:
            msg = (
                "❗ Ошибки обновления остатков в Ozon по отдельным товарам:\n"
                + json.dumps(errors, ensure_ascii=False)[:3000]
            )
            send_telegram_message(msg)
        except Exception:
            pass

    return data


def get_fbs_postings(limit: int = 50) -> dict:
    """
    Получение FBS-отправлений Ozon через /v3/posting/fbs/list.

    Берём отправления за последние 7 дней, по умолчанию limit штук.
    """
    url = f"{BASE_URL}/v3/posting/fbs/list"

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=7)

    body = {
        "dir": "DESC",
        "filter": {
            "since": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
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
    print(r.text)
    print("=== /Ответ Ozon ===\n")

    r.raise_for_status()
    return r.json()
