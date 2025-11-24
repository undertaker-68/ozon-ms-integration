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

def get_products_state_by_offer_ids(offer_ids):
    """
    Возвращает словарь {offer_id: state} для переданных offer_id.
    state, как правило: ACTIVE, ARCHIVED, DISABLED и т.п.
    """
    if not offer_ids:
        return {}

    url = f"{OZON_API_URL}/v2/products/info"

    # Ozon обычно позволяет до 1000 offer_id за раз, на всякий случай батчим
    BATCH_SIZE = 1000
    result = {}

    for i in range(0, len(offer_ids), BATCH_SIZE):
        batch = offer_ids[i:i + BATCH_SIZE]

        body = {
            "offer_id": batch,
            "product_id": [],
            "sku": []
        }

        print("=== Тело запроса к Ozon /v2/products/info ===")
        print(body)
        print("=== /Тело запроса ===\n")

        r = requests.post(url, json=body, headers=HEADERS)
        print("=== Ответ Ozon /v2/products/info ===")
        print("HTTP status:", r.status_code)
        try:
            data = r.json()
            print("JSON (фрагмент):", str(data)[:500])
        except Exception:
            data = {}
            print("TEXT:", r.text[:500])
        print("=== /Ответ Ozon ===\n")

        r.raise_for_status()

        items = data.get("result", [])
        for item in items:
            oid = item.get("offer_id")
            state = item.get("state")  # обычно тут ARCHIVED / ACTIVE / и т.п.
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
    OZON_API_URL = "https://api-seller.ozon.ru"

    if not stocks:
        print("update_stocks: передан пустой список stocks, запрос к Ozon не отправляется.")
        return {"result": []}

    url = f"{OZON_API_URL}/v2/products/stocks"
    body = {"stocks": stocks}

    print("=== Тело запроса к Ozon /v2/products/stocks ===")
    print(json.dumps(body, ensure_ascii=False, indent=2))
    print("=== /Тело запроса ===\n")

    try:
        r = requests.post(url, json=body, headers=HEADERS)
    except Exception as e:
        msg = f"❗ Ошибка сети при обновлении остатков в Ozon: {e}"
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


BASE_URL = "https://api-seller.ozon.ru"


def get_fbs_postings(limit: int = 10) -> dict:
    """
    Получить список FBS-отправлений за последние 7 дней.

    Используем поля filter.since и filter.to,
    как в официальном примере документации.
    """

    url = f"{BASE_URL}/v3/posting/fbs/list"

    now_utc = datetime.now(timezone.utc)
    week_ago = now_utc - timedelta(days=7)

    since = week_ago.isoformat(timespec="seconds").replace("+00:00", "Z")
    to = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")

    body = {
        "dir": "ASC",
        "filter": {
            "since": since,
            "to": to,
            # статус можно пока не указывать, чтобы увидеть все:
            # "status": "awaiting_packaging",
            # по желанию потом добавим warehouse_id и др. фильтры
        },
        "limit": limit,
        "offset": 0,
        "with": {
            "analytics_data": True,
            "financial_data": True,
        },
    }

    print("=== Тело запроса к Ozon /v3/posting/fbs/list ===")
    print(body)
    print("=== /Тело запроса ===\n")

    r = requests.post(url, json=body, headers=HEADERS)

    print("=== Ответ Ozon /v3/posting/fbs/list ===")
    print("HTTP status:", r.status_code)
    print("Response text:")
    print(r.text)
    print("=== /Ответ Ozon ===\n")

    r.raise_for_status()
    return r.json()
