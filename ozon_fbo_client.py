import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

try:
    from notifier import send_telegram_message
except Exception:  # noqa: BLE001
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

        self.client_id = str(client_id)
        self.api_key = str(api_key)
        self.account_name = account_name

        self.headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

    # --------------------------
    # БАЗОВЫЙ POST (+ retry 429)
    # --------------------------
    def _post(self, path: str, body: dict, max_retries: int = 3) -> dict:
        url = f"{OZON_API_URL}{path}"

        for attempt in range(1, max_retries + 1):
            r = requests.post(url, json=body, headers=self.headers, timeout=30)

            # Rate limit
            if r.status_code == 429:
                wait_s = 2 * attempt
                print(f"⚠ Лимит запросов Ozon (429) на {path} ({self.account_name}), попытка {attempt}/{max_retries}, ждём {wait_s}s")
                time.sleep(wait_s)
                continue

            # Валидации (очень полезно видеть текст)
            if r.status_code >= 400:
                try:
                    text = r.text
                except Exception:
                    text = "<no body>"
                print(f"❗ Ошибка Ozon {path} ({self.account_name}) HTTP {r.status_code}\n{text}")
                r.raise_for_status()

            return r.json()

        # если все попытки 429 исчерпаны
        msg = f"❗ Ozon {path}: 429 Too Many Requests, попытки исчерпаны ({self.account_name})"
        print(msg)
        try:
            send_telegram_message(msg)
        except Exception:
            pass
        raise requests.HTTPError(msg)

    # --------------------------
    # LIST: получить ID заявок
    # --------------------------
    def list_supply_order_ids(
        self,
        limit: int = 50,
        days_back: int = 30,
        states: Optional[List[str]] = None,
    ) -> List[int]:
        """
        Получить ID заявок на поставку FBO за последние N дней.

        Важно: /v3/supply-order/list строго валидирует filter.states.
        Если передать «не тот» статус (например DATA_FILLING),
        Ozon может вернуть 400. Поэтому здесь есть fallback.
        """
        if not isinstance(limit, int) or limit <= 0:
            return []

        now = datetime.now(timezone.utc)
        since = now - timedelta(days=days_back)

        # Безопасные статусы для /list (практика + твой curl)
        safe_states = [
            "CREATED",
            "READY_TO_SUPPLY",
            "IN_TRANSIT",
            "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
            "REPORTS_CONFIRMATION_AWAITING",
        ]

        use_states = states or safe_states
        # нормализуем
        use_states = [str(s).upper() for s in use_states if str(s).strip()]
        if not use_states:
            use_states = safe_states

        def _make_body(sts: List[str]) -> dict:
            return {
                "filter": {
                    "states": sts,
                    "from": since.isoformat(timespec="seconds").replace("+00:00", "Z"),
                    "to": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
                },
                "limit": min(limit, 50),
                "sort_by": "ORDER_CREATION",
                "sort_dir": "DESC",
            }

        body = _make_body(use_states)

        print(
            f"[OZON FBO] Запрос списка заявок на поставку ({self.account_name}), "
            f"limit={body['limit']}, days_back={days_back}"
        )

        try:
            data = self._post("/v3/supply-order/list", body)
        except requests.HTTPError:
            # fallback на безопасные состояния (если твои упали 400)
            if use_states != safe_states:
                print(f"[OZON FBO] Повторяем /list с безопасными states={safe_states} ({self.account_name})")
                data = self._post("/v3/supply-order/list", _make_body(safe_states))
            else:
                raise

        order_ids = data.get("order_ids") or []
        if not isinstance(order_ids, list):
            return []
        # приводим к int
        result: List[int] = []
        for x in order_ids:
            try:
                result.append(int(x))
            except Exception:
                continue
        return result

    # --------------------------
    # GET: детали заявок по ID
    # --------------------------
    def get_supply_orders(self, order_ids: List[int]) -> List[Dict[str, Any]]:
        if not order_ids:
            return []

        # Ozon: от 1 до 50 ids за раз
        chunk: List[List[int]] = []
        cur: List[int] = []
        for oid in order_ids:
            if len(cur) >= 50:
                chunk.append(cur)
                cur = []
            cur.append(int(oid))
        if cur:
            chunk.append(cur)

        all_orders: List[Dict[str, Any]] = []
        for part in chunk:
            body = {"order_ids": part}
            data = self._post("/v3/supply-order/get", body)
            orders = data.get("orders") or []
            if isinstance(orders, list):
                all_orders.extend([o for o in orders if isinstance(o, dict)])

        return all_orders

    # --------------------------
    # bundle: состав поставки
    # --------------------------
    def get_bundle_items(self, bundle_id: str) -> List[Dict[str, Any]]:
        if not bundle_id:
            return []
        body = {"bundle_id": bundle_id, "limit": 1000}
        data = self._post("/v1/supply-order/bundle", body)
        items = data.get("items") or []
        if not isinstance(items, list):
            return []
        return [x for x in items if isinstance(x, dict)]
