import time
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

OZON_API_URL = "https://api-seller.ozon.ru"


class OzonFboClient:
    def __init__(self, client_id: str, api_key: str, account_name: str = "ozon") -> None:
        self.client_id = client_id
        self.api_key = api_key
        self.account_name = account_name

        self.headers = {
            "Client-Id": client_id,
            "Api-Key": api_key,
            "Content-Type": "application/json",
        }

    def _post(self, path: str, body: dict) -> dict:
        url = f"{OZON_API_URL}{path}"
        for attempt in range(3):
            r = requests.post(url, json=body, headers=self.headers, timeout=30)
            if r.status_code == 429:
                time.sleep(1 + attempt * 2)
                continue
            r.raise_for_status()
            return r.json()
        r.raise_for_status()

    def list_supply_order_ids(self, limit=50, days_back=30, states=None) -> list[int]:
        now = datetime.now(timezone.utc)
        since = now - timedelta(days=days_back)

        body = {
            "filter": {
                "states": states or [],
                "from": since.isoformat().replace("+00:00", "Z"),
                "to": now.isoformat().replace("+00:00", "Z"),
            },
            "limit": min(limit, 50),
        }

        data = self._post("/v3/supply-order/list", body)
        return [int(x) for x in data.get("order_ids", [])]

    def get_supply_orders_by_ids(self, order_ids: list[int]) -> list[dict]:
        if not order_ids:
            return []
        data = self._post("/v3/supply-order/get", {"order_ids": order_ids})
        return data.get("orders", [])

    def get_bundle_items(self, bundle_id: str) -> list[dict]:
        data = self._post("/v1/supply-order/bundle", {"bundle_ids": [bundle_id]})
        return data.get("items", [])
