import os
import json
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

try:
    from notifier import send_telegram_message
except ImportError:
    def send_telegram_message(text: str) -> bool:  # type: ignore
        print("Telegram notifier не доступен:", text)
        return False


load_dotenv()

# ==========================
# БАЗОВЫЕ НАСТРОЙКИ OZON FBO
# ==========================

OZON_API_URL = "https://api-seller.ozon.ru"


class OzonFboClient:
    """
    Мини-клиент только под FBO-поставки.

    Использует:
      * /v3/supply-order/list — список заявок на поставку
      * /v3/supply-order/get  — детали заявки и список поставок в ней
      * /v1/supply-order/bundle — состав поставки (товары)
    """

    def __init__(self, client_id: str, api_key: str, account_name: str = "ozon1") -> None:
        if not client_id or not api_key:
            raise RuntimeError("Не заданы Client-Id / Api-Key для Ozon FBO")

        self.client_id = client_id
        self.api_key = api_key
        self.account_name = account_name

        self.headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

    # --------------------------
    # ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
    # --------------------------

    def _post(self, path: str, body: dict) -> dict:
        url = f"{OZON_API_URL}{path}"

        try:
            r = requests.post(url, json=body, headers=self.headers, timeout=30)
        except Exception as e:  # noqa: BLE001
            msg = f"❗ Ошибка запроса к Ozon {path} ({self.account_name}): {e!r}"
            print(msg)
            try:
                send_telegram_message(msg)
            except Exception:
                pass
            raise

        if r.status_code != 200:
            msg = (
                f"❗ Ошибка Ozon {path} ({self.account_name})\n"
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
            return r.json()
        except Exception as e:  # noqa: BLE001
            msg = f"❗ Не удалось распарсить JSON от Ozon {path} ({self.account_name}): {e!r}"
            print(msg)
            try:
                send_telegram_message(msg)
            except Exception:
                pass
            raise

    # --------------------------
    # FBO: СПИСОК ЗАЯВОК
    # --------------------------

    def list_supply_order_ids(
        self,
        limit: int = 100,
        days_back: int = 30,
        states: list[str] | None = None,
    ) -> list[int]:
        """
        Получить ID заявок на поставку FBO за последние N дней.

        Использует /v3/supply-order/list.
        """
        if limit <= 0:
            return []

        now = datetime.now(timezone.utc)
        since = now - timedelta(days=days_back)

        # По документации допустимые статусы:
        #   DATA_FILLING, READY_TO_SUPPLY, ACCEPTED_AT_SUPPLY_WAREHOUSE,
        #   IN_TRANSIT, ACCEPTANCE_AT_STORAGE_WAREHOUSE,
        #   REPORTS_CONFIRMATION_AWAITING, REPORT_REJECTED,
        #   COMPLETED, REJECTED_AT_SUPPLY_WAREHOUSE,
        #   CANCELLED, OVERDUE
        if states is None:
            states = [
                "DATA_FILLING",
                "READY_TO_SUPPLY",
                "ACCEPTED_AT_SUPPLY_WAREHOUSE",
                "IN_TRANSIT",
                "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
                "REPORTS_CONFIRMATION_AWAITING",
                "REPORT_REJECTED",
                "COMPLETED",
            ]

        body = {
            "filter": {
                "states": states,
                "from": since.isoformat(timespec="seconds").replace("+00:00", "Z"),
                "to": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
            "limit": limit,
            "sort_by": "ORDER_CREATION",
            "sort_dir": "DESC",
        }

        print(
            f"[OZON FBO] Запрос списка заявок на поставку "
            f"({self.account_name}), limit={limit}, days_back={days_back}"
        )
        data = self._post("/v3/supply-order/list", body)

        order_ids = data.get("order_ids") or []
        if not isinstance(order_ids, list):
            print(
                "[OZON FBO] Неожиданный формат ответа /v3/supply-order/list: "
                f"order_ids={order_ids!r}"
            )
            return []

        print(
            f"[OZON FBO] Получено заявок на поставку ({self.account_name}): "
            f"{len(order_ids)}"
        )
        return [oid for oid in order_ids if isinstance(oid, int)]

    # --------------------------
    # FBO: ДЕТАЛИ ЗАЯВКИ
    # --------------------------

    def get_supply_order(self, order_id: int) -> dict:
        """
        Получить полную информацию о заявке на поставку.
        Использует /v3/supply-order/get.
        """
        body = {"order_id": order_id}
        data = self._post("/v3/supply-order/get", body)

        # Немного "обогащаем" объект для удобства
        data["_ozon_account"] = self.account_name
        data["_order_id"] = order_id
        return data

    def get_supply_orders(
        self,
        limit: int = 100,
        days_back: int = 30,
        states: list[str] | None = None,
    ) -> list[dict]:
        """
        Получить список детальных заявок на поставку.
        """
        order_ids = self.list_supply_order_ids(limit=limit, days_back=days_back, states=states)
        result: list[dict] = []

        for order_id in order_ids:
            try:
                order = self.get_supply_order(order_id)
            except Exception as e:  # noqa: BLE001
                print(f"[OZON FBO] Ошибка получения заявки {order_id} ({self.account_name}): {e!r}")
                continue
            result.append(order)

        return result

    # --------------------------
    # FBO: СОСТАВ ПОСТАВКИ
    # --------------------------

    def get_bundle_items(self, bundle_id: str, limit: int = 100) -> list[dict]:
        """
        Получить список товаров по bundle_id (часть поставки).
        Использует /v1/supply-order/bundle.
        """
        if not bundle_id:
            return []

        items: list[dict] = []
        last_id: str | None = None

        while True:
            body = {
                "bundle_ids": [bundle_id],
                "limit": limit,
                "is_asc": True,
            }
            if last_id:
                body["last_id"] = last_id

            data = self._post("/v1/supply-order/bundle", body)

            batch = data.get("items") or []
            if not isinstance(batch, list):
                break

            items.extend(batch)

            has_next = bool(data.get("has_next"))
            last_id = data.get("last_id") or None

            if not has_next or not last_id:
                break

        print(
            f"[OZON FBO] Для bundle_id={bundle_id} ({self.account_name}) "
            f"получено товаров: {len(items)}"
        )
        return items
