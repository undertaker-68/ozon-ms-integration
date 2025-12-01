import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api-seller.ozon.ru"

def fetch_info(headers, offer_ids, label):
    payload = {
        "offer_id": offer_ids,
        "product_id": [],
        "sku": []
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
        print(resp.text)
        return

    # Аккуратно сокращаем вывод
    items = data.get("items", [])
    for item in items:
        print("\noffer_id:", item.get("offer_id"))
        print("name:", item.get("name"))
        print("is_archived:", item.get("is_archived"))
        print("is_autoarchived:", item.get("is_autoarchived"))
        statuses = item.get("statuses", {})
        print("statuses.status:", statuses.get("status"))
        print("statuses.status_name:", statuses.get("status_name"))
        print("statuses.status_description:", statuses.get("status_description"))
        # Если можешь, оставь ещё полный кусок для проблемного товара:
        # print(json.dumps(item, ensure_ascii=False, indent=2))


def main():
    offer_ids = ["VW12"]  # сюда можно добавить ещё проблемные offer_id

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
