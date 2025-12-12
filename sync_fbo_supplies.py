import os
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from requests.exceptions import HTTPError

from ozon_fbo_client import OzonFboClient
from ms_client import (
    find_product_by_article,
    create_customer_order,
    find_customer_order_by_name,
    update_customer_order,
)

load_dotenv()

# ==========================
# НАСТРОЙКИ
# ==========================

DRY_RUN_FBO = os.getenv("DRY_RUN_FBO", "false").lower() == "true"

# Две текущие поставки должны обновляться всегда (как ты хотел)
PINNED_ORDER_NUMBERS = {"2000037545485", "2000037485754"}

# Cutoff-файл: всё, что создано раньше cutoff, не трогаем (кроме pinned)
FBO_CUTOFF_FILE = os.getenv("FBO_CUTOFF_FILE", "fbo_cutoff.json")

# Только “Подготовка к поставкам”
PREP_STATES = {"DATA_FILLING", "READY_TO_SUPPLY"}

# МойСклад
MS_ORGANIZATION_HREF = os.getenv("MS_ORGANIZATION_HREF")
MS_AGENT_HREF = os.getenv("MS_AGENT_HREF")
MS_FBO_STORE_HREF = os.getenv("MS_FBO_STORE_HREF") or os.getenv("MS_STORE_HREF")

# Статус заказа покупателя “FBO”
MS_STATE_FBO_HREF = os.getenv("MS_STATE_FBO_HREF") or os.getenv("MS_STATE_FBO")

if not MS_ORGANIZATION_HREF or not MS_AGENT_HREF or not MS_FBO_STORE_HREF:
    raise RuntimeError(
        "Не заданы MS_ORGANIZATION_HREF / MS_AGENT_HREF / MS_FBO_STORE_HREF "
        "(или MS_STORE_HREF). Проверь .env"
    )

# ==========================
# ВСПОМОГАТЕЛЬНЫЕ
# ==========================

def _parse_ozon_dt(s: Optional[str]) -> Optional[datetime]:
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _to_ms_moment(dt: Optional[datetime]) -> Optional[str]:
    """
    МойСклад принимает plannedMoment в формате 'YYYY-MM-DD HH:MM:SS'
    """
    if not dt:
        return None
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _ms_meta(href: str, type_: str) -> Dict[str, Any]:
    return {"href": href, "type": type_, "mediaType": "application/json"}


def _cluster_from_storage_name(storage_name: str) -> str:
    up = (storage_name or "").upper()
    if "ПУШКИНО" in up:
        return "Москва и МО"
    # если склад в формате ГОРОД_..., берём “ГОРОД”
    if "_" in (storage_name or ""):
        return storage_name.split("_", 1)[0]
    return storage_name or "—"


def _load_cutoff() -> Optional[datetime]:
    if not os.path.exists(FBO_CUTOFF_FILE):
        return None
    try:
        with open(FBO_CUTOFF_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        s = data.get("cutoff")
        return _parse_ozon_dt(s)
    except Exception:
        return None


def _save_cutoff(dt: datetime) -> None:
    dt = dt.astimezone(timezone.utc)
    s = dt.isoformat().replace("+00:00", "Z")
    with open(FBO_CUTOFF_FILE, "w", encoding="utf-8") as f:
        json.dump({"cutoff": s}, f, ensure_ascii=False, indent=2)


def _ms_call_retry(fn, *args, **kwargs):
    """
    Ретрай на 429 от МойСклад, чтобы скрипт не умирал на лимитах.
    """
    max_tries = 5
    backoff = 0.8
    for attempt in range(1, max_tries + 1):
        try:
            return fn(*args, **kwargs)
        except HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code == 429:
                wait = backoff * attempt
                print(f"[MS] 429 rate limit, попытка {attempt}/{max_tries}, ждём {wait:.1f}s")
                time.sleep(wait)
                continue
            raise
        except Exception:
            # Если ms_client не пробрасывает HTTPError как надо,
            # но в тексте встречается 429 — тоже перетерпим.
            msg = repr(_) if False else ""  # no-op to appease linters
            text = ""
            try:
                text = str(e)
            except Exception:
                text = repr(e)
            if " 429 " in text or "status=429" in text or "Превышено ограничение" in text:
                wait = backoff * attempt
                print(f"[MS] Похоже на 429, попытка {attempt}/{max_tries}, ждём {wait:.1f}s")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("[MS] Не удалось выполнить запрос из-за постоянных 429")


def _get_planned_dt(order: Dict[str, Any]) -> Optional[datetime]:
    """
    Планируемая дата = arrival_date склада назначения, если есть.
    Иначе — created_date.
    """
    supplies = order.get("supplies") or []
    if isinstance(supplies, list) and supplies:
        s0 = supplies[0] if isinstance(supplies[0], dict) else {}
        storage = s0.get("storage_warehouse") or {}
        if isinstance(storage, dict):
            arrival = storage.get("arrival_date")
            dt = _parse_ozon_dt(arrival)
            if dt:
                return dt
    return _parse_ozon_dt(order.get("created_date"))


def _collect_positions(order: Dict[str, Any], client: OzonFboClient) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Берём товары через /v1/supply-order/bundle по bundle_id.
    Сопоставляем по артикулу продавца: offer_id/vendor_code/contractor_item_code.
    Цену ставим из МойСклад salePrices[0].value.
    """
    positions: List[Dict[str, Any]] = []
    errors: List[str] = []

    supplies = order.get("supplies") or []
    if not isinstance(supplies, list):
        supplies = []

    for sup in supplies:
        if not isinstance(sup, dict):
            continue
        bundle_id = sup.get("bundle_id")
        if not bundle_id:
            continue

        items = client.get_bundle_items(bundle_id)
        print(f"[OZON FBO] Для bundle_id={bundle_id} ({client.account_name}) получено товаров: {len(items)}")

        for it in items:
            if not isinstance(it, dict):
                continue

            offer = (
                it.get("offer_id")
                or it.get("vendor_code")
                or it.get("contractor_item_code")
            )
            if offer is None or str(offer).strip() == "":
                # fallback (хуже): sku
                sku = it.get("sku")
                if sku is not None:
                    offer = str(sku)

            if not offer:
                continue

            offer = str(offer).strip()
            qty = it.get("quantity") or 0
            try:
                qty = int(qty)
            except Exception:
                qty = 0
            if qty <= 0:
                continue

            product = _ms_call_retry(find_product_by_article, offer)
            if not product:
                errors.append(f"Товар с артикулом '{offer}' не найден в МойСклад")
                continue

            price = None
            sale_prices = product.get("salePrices")
            if isinstance(sale_prices, list) and sale_prices:
                first = sale_prices[0] or {}
                price = first.get("value")

            pos = {
                "quantity": qty,
                "assortment": {"meta": product["meta"]},
            }
            if price is not None:
                pos["price"] = price

            positions.append(pos)

    return positions, errors


# ==========================
# ОБРАБОТКА ОДНОЙ ПОСТАВКИ
# ==========================

def _process_one(order: Dict[str, Any], client: OzonFboClient, cutoff: Optional[datetime], dry_run: bool) -> None:
    order_number = str(order.get("order_number") or order.get("order_id") or "")
    state = str(order.get("state") or "").upper()
    created_dt = _parse_ozon_dt(order.get("created_date"))

    # pinned обрабатываем всегда
    if order_number not in PINNED_ORDER_NUMBERS:
        # остальные — только если created_date >= cutoff
        if cutoff and created_dt and created_dt < cutoff:
            return

    # Только “подготовка к поставкам”
    if state not in PREP_STATES:
        return

    planned_dt = _get_planned_dt(order)
    planned_ms = _to_ms_moment(planned_dt)

    supplies = order.get("supplies") or []
    storage_name = "—"
    if isinstance(supplies, list) and supplies and isinstance(supplies[0], dict):
        storage = supplies[0].get("storage_warehouse") or {}
        if isinstance(storage, dict):
            storage_name = storage.get("name") or "—"

    cluster = _cluster_from_storage_name(storage_name)

    # Комментарий: БЕЗ склада отгрузки (Красноярск и т.п. не используем вообще)
    comment = f"{order_number} - {cluster} - {storage_name}"

    positions_payload, pos_errors = _collect_positions(order, client)

    print(
        f"[FBO] Обработка заявки {order_number} "
        f"(аккаунт={client.account_name}, state={state}), "
        f"позиций={len(positions_payload)}, DRY_RUN={dry_run}"
    )

    if not positions_payload:
        if pos_errors:
            print(f"[FBO] {order_number}: нет позиций МС. Примеры ошибок: {pos_errors[:5]}")
        else:
            print(f"[FBO] {order_number}: нет позиций МС (без деталей)")
        return

    payload: Dict[str, Any] = {
        "name": order_number,
        "organization": {"meta": _ms_meta(MS_ORGANIZATION_HREF, "organization")},
        "agent": {"meta": _ms_meta(MS_AGENT_HREF, "counterparty")},
        "store": {"meta": _ms_meta(MS_FBO_STORE_HREF, "store")},
        "description": comment,
        "positions": positions_payload,
    }

    # Планируемая дата отгрузки (и доставки — на всякий случай)
    if planned_ms:
        payload["shipmentPlannedMoment"] = planned_ms
        payload["deliveryPlannedMoment"] = planned_ms

    # Статус заказа FBO
    if MS_STATE_FBO_HREF:
        payload["state"] = {"meta": _ms_meta(MS_STATE_FBO_HREF, "state")}

    if dry_run:
        return

    # мягко ограничим RPS к МС
    time.sleep(0.25)

    existing = _ms_call_retry(find_customer_order_by_name, order_number)
    if existing:
        href = existing["meta"]["href"]
        _ms_call_retry(update_customer_order, href, payload)
        print(f"[FBO] Заказ {order_number} обновлён в МС")
    else:
        _ms_call_retry(create_customer_order, payload)
        print(f"[FBO] Заказ {order_number} создан в МС")


# ==========================
# ОСНОВНОЙ ЗАПУСК
# ==========================

def sync_fbo_supplies(limit: int = 50, days_back: int = 30, dry_run: bool = False) -> None:
    print(
        f"Запуск синхронизации FBO-поставок "
        f"(limit={limit}, days_back={days_back}, DRY_RUN={dry_run})"
    )

    clients: List[OzonFboClient] = []

    oz1_id = os.getenv("OZON_CLIENT_ID")
    oz1_key = os.getenv("OZON_API_KEY")
    if oz1_id and oz1_key:
        clients.append(OzonFboClient(oz1_id, oz1_key, account_name="ozon1"))

    oz2_id = os.getenv("OZON2_CLIENT_ID")
    oz2_key = os.getenv("OZON2_API_KEY")
    if oz2_id and oz2_key:
        clients.append(OzonFboClient(oz2_id, oz2_key, account_name="ozon2"))

    if not clients:
        print("[FBO] Нет настроенных кабинетов Ozon для FBO (проверь .env)")
        return

    cutoff = _load_cutoff()
    if cutoff is None and not dry_run:
        cutoff = datetime.now(timezone.utc)
        _save_cutoff(cutoff)
        print(f"[FBO] Установлена отсечка для новых поставок: {cutoff.isoformat()}")
    else:
        print(f"[FBO] Текущая отсечка: {cutoff.isoformat() if cutoff else 'нет (DRY_RUN?)'}")

    for client in clients:
        try:
            orders = client.get_supply_orders(limit=limit, days_back=days_back)
        except Exception as e:
            print(f"[FBO] Ошибка получения списка поставок ({client.account_name}): {e!r}")
            continue

        print(f"[FBO] Кабинет {client.account_name}: получено заявок: {len(orders)}")

        for order in orders:
            try:
                _process_one(order, client, cutoff=cutoff, dry_run=dry_run)
            except Exception as e:
                num = str(order.get("order_number") or order.get("order_id") or "")
                print(f"[FBO] Ошибка обработки заявки {num} ({client.account_name}): {e!r}")
                continue


if __name__ == "__main__":
    sync_fbo_supplies(limit=50, days_back=30, dry_run=DRY_RUN_FBO)
