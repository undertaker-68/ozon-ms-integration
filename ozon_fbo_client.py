# ozon_fbo_client.py
import os
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID")
OZON_API_KEY = os.getenv("OZON_API_KEY")

OZON2_CLIENT_ID = os.getenv("OZON2_CLIENT_ID")
OZON2_API_KEY = os.getenv("OZON2_API_KEY")

BASE_URL = "https://api-seller.ozon.ru"


def _get_headers(ozon_account: str) -> dict:
    if ozon_account == "ozon2":
        client_id = OZON2_CLIENT_ID
        api_key = OZON2_API_KEY
    else:
        client_id = OZON_CLIENT_ID
        api_key = OZON_API_KEY

    return {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }


def get_fbo_supplies(ozon_account: str, days: int = 14) -> list[dict]:
    """
    Получить список FBO-поставок за последние N дней.
    Здесь используем /v3/supply-order/list (актуальная версия).
    """
    import json  # локальный импорт, чтобы не тащить наверх

    headers = _get_headers(ozon_account)
    url = f"{BASE_URL}/v3/supply-order/list"

    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=days)).isoformat()
    date_to = now.isoformat()

    body = {
        "filter": {
            # фильтры бери из PDF-документации Ozon:
            # статус(ы), дата создания/таймслот и т.п.
            "date_created_from": date_from,
            "date_created_to": date_to,
            # "status": [...],  # если нужно ограничить
        },
        "limit": 1000,
        "offset": 0,
    }

    resp = requests.post(url, json=body, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Смотри точное поле в документации Ozon.
    # Обычно это что-то вроде data["result"]["supply_orders"]
    supplies = (data.get("result") or {}).get("supply_orders") or []
    return supplies


def get_fbo_supply_items(ozon_account: str, supply_id: str) -> list[dict]:
    """
    Получить состав FBO-поставки.
    Используем /v1/supply-order/items (или более свежую версию, если появится).
    """
    headers = _get_headers(ozon_account)
    url = f"{BASE_URL}/v1/supply-order/items"

    body = {
        "supply_order_id": supply_id,
    }

    resp = requests.post(url, json=body, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Точное поле также смотри в документации:
    # обычно data["result"]["items"]
    items = (data.get("result") or {}).get("items") or []
    return items
