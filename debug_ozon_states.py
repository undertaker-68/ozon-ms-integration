import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api-seller.ozon.ru"


def fetch_info(headers, offer_ids, label):
    payload = {
        "offer_id": offer_ids,
        # НИЧЕГО лишнего — только offer_id
    }
    resp = requests.post(
        BASE_URL + "/v3/product/info/list",
        headers=headers,
        json=payload,
        timeout=60,
    )
    print(f"\n=== {label} ===")
    print("HTTP:", resp.status_code)

    try:
        data = resp.json()
    except Exception:
        print("RAW TEXT:", resp.text[:2000])
        return

    print("TOP-LEVEL KEYS:", list(data.keys()))

    # В разных версиях API бывает items или result
    items = data.get("items") or data.get("result") or []

    print(f"items count: {len(items)}")

    for item in items:
        oid = item.get("offer_id")
        if oid not in offer_ids:
            continue

        print("\n------ ITEM ------")
        print("offer_id:", item.get("offer_id"))
        print("name:", item.get("name"))
        print("is_archived:", item.get("is_archived"))
        print("is_autoarchived:", item.get("is_autoarchived"))

        statuses = item.get("statuses") or {}
        print("STATUSES RAW:", statuses)

        # На всякий случай выведем state/status, если они сверху лежат
        print("item.state:", item.get("state"))
        print("item.status:", item.get("status"))

        # И можешь для проблемных товаров раскомментировать:
        # print(json.dumps(item, ensure_ascii=False, indent=2))


def main():
    # сюда можно добавить ещё проблемные offer_id
    offer_ids = ["VW12"]

    headers1 = {
        "Client-Id": os.getenv("OZON_CLIENT_ID"),
        "Api-Key": os.getenv("OZON_API_KEY"),
        "Content-Type": "application/json",
    }
    headers2 = {
        "Client-Id": os.getenv("OZON2_CLIENT_ID"),
        "Api-Key": os.getenv("OZON2_API_KEY"),
        "Content-Type": "application/json",
    }

    fetch_info(headers1, offer_ids, "OZON #1 (Auto-MiX)")
    fetch_info(headers2, offer_ids, "OZON #2 (Trail Gear)")


if __name__ == "__main__":
    main()
