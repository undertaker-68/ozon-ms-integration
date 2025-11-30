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


def get_fbs_postings(limit: int = 3) -> dict:
    """
    Получение FBS-отправлений для ВТОРОГО кабинета Ozon
    за последние 7 дней. Логика та же, что и в основном ozon_client.
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
        postings = data.get("result", {}).get("postings", [])

        for p in postings:
            pn = p.get("posting_number")
            if not pn or pn in seen_numbers:
                continue
            seen_numbers.add(pn)

            # Помечаем, что это второй кабинет
            p["_ozon_account"] = "ozon2"

            all_postings.append(p)
            if len(all_postings) >= limit:
                break

    print(f"[OZON2] Получено FBS-отправлений: {len(all_postings)}")
    return {"result": {"postings": all_postings}}
