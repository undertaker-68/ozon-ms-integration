import os
from datetime import datetime, timedelta, timezone
import time

import requests
from dotenv import load_dotenv

try:
    from notifier import send_telegram_message
except ImportError:
    def send_telegram_message(text: str) -> bool:  # type: ignore
        print("Telegram notifier не доступен:", text)
        return False


load_dotenv()

OZON_API_URL = "https://api-seller.ozon.ru"


class OzonFboClient:
    """
    Мини-клиент только под FBO-поставки.

    Использует:
      * /v3/supply-order/list — список заявок на поставку (order_ids)
      * /v3/supply-order/get  — детали заявок по списку order_ids
      * /v1/supply-order/bundle — состав поставки по bundle_id
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
    # БАЗОВЫЙ POST
    # --------------------------
        def get_supply_orders_by_ids(self, order_ids: list[int]) -> list[dict]:
        if not order_ids:
            return []

        body = {
            "order_ids": order_ids
        }

        data = self._post("/v3/supply-order/get", body)
        return data.get("orders") or []

    def _post(self, path: str, body: dict) -> dict:
        """
        Базовый POST с обработкой 429 (rate limit) через небольшие паузы и повторы.
        """
        url = f"{OZON_API_URL}{path}"

        max_retries = 3
        for attempt in range(max_retries):
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

            # Лимит запросов — делаем паузу и повторяем
            if r.status_code == 429 and attempt < max_retries - 1:
                msg = (
                    f"⚠ Лимит запросов Ozon (429) на {path} "
                    f"({self.account_name}), попытка {attempt + 1}/{max_retries}"
                )
                print(msg)
                try:
                    send_telegram_message(msg)
                except Exception:
                    pass

                # небольшая экспоненциальная пауза: 1s, 3s, 5s
                time.sleep(1 + attempt * 2)
                continue

            # выходим из цикла — либо нормальный код, либо не 429
            break

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
    # FBO: СПИСОК ID ЗАЯВОК
    # --------------------------

    def list_supply_order_ids(
        self,
        limit: int = 100,
        days_back: int = 30,
        states: list[str] | None = None,
    ) -> list[int]:
        """
        Получить ID заявок на поставку FBO за последние N дней.

        ВАЖНО: мы *не* берём CANCELLED / COMPLETED,
        чтобы не трогать удалённые/законченные поставки.
        """
        if limit <= 0:
            return []

        now = datetime.now(timezone.utc)
        since = now - timedelta(days=days_back)

        # Статусы для "Подготовка к поставкам" + в пути/приёмка
        if states is None:
            states = [
                "DATA_FILLING",                    # черновик / заполнение
                "CREATED",                         # создана
                "READY_TO_SUPPLY",                 # готова к поставке
                "ACCEPTED_AT_SUPPLY_WAREHOUSE",    # принята на складе поставки
                "IN_TRANSIT",                      # в пути
                "ACCEPTANCE_AT_STORAGE_WAREHOUSE", # приёмка на склад хранения
                "REPORTS_CONFIRMATION_AWAITING",   # ждёт подтверждения отчётов
            ]

        body = {
            "filter": {
                "states": states,
                "from": since.isoformat(timespec="seconds").replace("+00:00", "Z"),
                "to": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
            "limit": min(limit, 50),
            "sort_by": "ORDER_CREATION",
            "sort_dir": "DESC",
        }

        print(
            f"[OZON FBO] Запрос списка заявок на поставку "
            f"({self.account_name}), limit={body['limit']}, days_back={days_back}"
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
            f"[OZON FBO] Получено заявок на поставку (IDs) ({self.account_name}): "
            f"{len(order_ids)}"
        )
        return [
            int(oid)
            for oid in order_ids
            if isinstance(oid, int) or (isinstance(oid, str) and oid.isdigit())
        ]

    # --------------------------
    # FBO: ДЕТАЛИ ЗАЯВОК
    # --------------------------

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
        if not order_ids:
            return []

        result: list[dict] = []
        chunk_size = 50

        for i in range(0, len(order_ids), chunk_size):
            chunk = order_ids[i : i + chunk_size]
            body = {"order_ids": chunk}

            print(
                f"[OZON FBO] Получение деталей заявок (get) "
                f"({self.account_name}), ids={chunk}"
            )
            data = self._post("/v3/supply-order/get", body)

            orders = data.get("orders") or data.get("result", {}).get("orders") or []
            if not isinstance(orders, list):
                print(
                    "[OZON FBO] Неожиданный формат ответа /v3/supply-order/get: "
                    f"orders={orders!r}"
                )
                continue

            for o in orders:
                o["_ozon_account"] = self.account_name
                o["_order_id"] = o.get("order_id") or o.get("id")
                result.append(o)

        print(
            f"[OZON FBO] Всего заявок с деталями ({self.account_name}): "
            f"{len(result)}"
        )
        return result

    # --------------------------
    # FBO: СОСТАВ ПОСТАВКИ
    # --------------------------

    def get_bundle_items(self, bundle_id: str, limit: int = 100) -> list[dict]:
        """
        Получить список товаров по bundle_id (часть поставки).
        /v1/supply-order/bundle
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

            try:
                data = self._post("/v1/supply-order/bundle", body)
            except requests.HTTPError as e:
                # Если даже после ретраев 429 — просто пропускаем эту поставку
                if e.response is not None and e.response.status_code == 429:
                    print(
                        f"[OZON FBO] Достигнут лимит запросов по bundle_id={bundle_id} "
                        f"({self.account_name}), пропускаем эту поставку в этом прогоне."
                    )
                    break
                raise

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
