import os
import csv
import json
from datetime import datetime
import requests
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

DRY_RUN_ORDERS = os.getenv("DRY_RUN_ORDERS", "true").lower() == "true"

# Добавляем параметры для работы с Ozon API
OZON_API_URL = "https://api-seller.ozon.ru/v3/"  # Окончательная версия API Ozon
OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID")  # Получить из переменной окружения
OZON_API_KEY = os.getenv("OZON_API_KEY")  # Получить из переменной окружения

ERRORS_FILE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "orders_errors.csv",
)

def _human_error_from_exception(e: Exception) -> str:
    """Функция для преобразования ошибок в читаемые сообщения для пользователя"""
    if isinstance(e, requests.HTTPError):
        resp = e.response
        status = resp.status_code if resp is not None else None
        text = ""
        data = None

        if resp is not None:
            text = resp.text or ""
            try:
                data = resp.json()
            except Exception:
                data = None

        if status == 412 and data:
            errors = data.get("errors") or []
            if errors:
                err_msg = errors[0].get("error") or errors[0].get("message") or ""
                if "Нельзя отгрузить товар, которого нет на складе" in err_msg:
                    return (
                        "МойСклад: нельзя отгрузить товар, которого нет на складе "
                        "(остаток по складу Ozon = 0 или меньше)."
                    )
                return f"МойСклад вернул ошибку 412: {err_msg}"

        if data and isinstance(data, dict) and data.get("errors"):
            parts = []
            for err in data["errors"]:
                msg = err.get("error") or err.get("message")
                if msg:
                    parts.append(msg)
            if parts:
                return f"HTTP {status or ''}: " + "; ".join(parts)

        return f"HTTP ошибка {status or ''} при обращении к API (подробности в логах)."

    msg = str(e)
    if "str' object does not support item assignment" in msg:
        return (
            "Внутренняя ошибка скрипта интеграции (TypeError: попытка изменить строку). "
            "Нужно исправить логику формирования данных перед отправкой."
        )

    return f"Неизвестная ошибка обработки отправления: {msg}"

def _append_order_errors_to_file(rows: list[dict]) -> None:
    """Записываем ошибку в CSV файл"""
    if not rows:
        return

    file_exists = os.path.exists(ERRORS_FILE_PATH)

    with open(ERRORS_FILE_PATH, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        if not file_exists:
            writer.writerow(["Дата/время", "Номер заказа", "Артикул", "Название", "Причина ошибки"])

        for r in rows:
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                r.get("posting_number", ""),
                r.get("article", ""),
                r.get("name", ""),
                r.get("reason", ""),
            ])

def _build_error_rows_for_posting(posting: dict, reason: str) -> list[dict]:
    """Строим строки для ошибки по одному заказу"""
    posting_number = posting.get("posting_number", "")
    products = posting.get("products") or []

    rows: list[dict] = []

    if products:
        for p in products:
            rows.append(
                {
                    "posting_number": posting_number,
                    "article": p.get("offer_id") or "",
                    "name": p.get("name") or "",
                    "reason": reason,
                }
            )
    else:
        rows.append(
            {
                "posting_number": posting_number,
                "article": "",
                "name": "",
                "reason": reason,
            }
        )

    return rows

def is_discounted_product(ozon_product: dict) -> bool:
    """Определяем, является ли товар уценённым на основе скидки или других характеристик"""
    price = ozon_product.get('price', 0)
    discounted_price = ozon_product.get('discount_price', 0)
    
    if discounted_price and discounted_price < price:
        return True
    return False

# Исправленный код с правильным импортом
def get_ozon_orders(limit: int = 10):
    """Получаем заказы с Ozon"""
    url = f"{OZON_API_URL}posting/fbs/list"
    headers = {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json"
    }

    # Получаем текущую дату и время в формате ISO 8601: 'YYYY-MM-DDTHH:MM:SS'
    processed_at_from = datetime.datetime.now().isoformat()

    payload = {
        "filter": {
            "status": "DELIVERING",  # Заказы в процессе доставки
            "limit": limit,
            "processedAtFrom": processed_at_from  # Используем формат ISO 8601
        }
    }

    try:
        # Отправляем запрос
        response = requests.post(url, headers=headers, json=payload)
        print("Ответ от Ozon:", response.text)  # Логирование ответа для отладки
        
        response.raise_for_status()  # Проверка на успешный ответ
        data = response.json()
        
        if 'result' in data:
            print(f"Получены заказы: {len(data['result'])}")
            return data['result']
        else:
            print(f"Ошибка: Нет поля 'result' в ответе Ozon. Ответ: {data}")
            return []
        
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе заказов с Ozon: {str(e)}")
        return []

def process_posting(posting: dict, dry_run: bool = True):
    """Обрабатываем отправление Ozon"""
    posting_number = posting.get("posting_number")
    status = posting.get("status")
    print(f"[ORDERS] Обработка {posting_number}, статус: {status}")

    if status != "delivering":
        print(f"[ORDERS] Статус не 'Доставляется' для {posting_number}, пропускаем.")
        return

    products = posting.get("products", [])
    for product in products:
        offer_id = product.get("offer_id", "")
        print(f"[MS] Найден в {offer_id}: {product.get('name')}")

        if is_discounted_product(product):
            print(f"[MS] Уценённый товар: {offer_id}, {product.get('name')}")
            product['quantity'] = 1  # отправляем 1 ед.

        try:
            create_demand_from_order(posting_number)
        except Exception as e:
            reason = _human_error_from_exception(e)
            print(f"[ORDERS] ❗ Ошибка обработки отправления {posting_number} | причина: {reason}")
            error_rows = _build_error_rows_for_posting(posting, reason)
            _append_order_errors_to_file(error_rows)

            msg = f"❗ Ошибка обработки отправления Ozon\nНомер отправления: {posting_number}\nПричина: {reason}"
            send_telegram_message(msg)

def sync_fbs_orders(dry_run: bool = True, limit: int = 10):
    """Основная функция для синхронизации заказов FBS"""
    postings = get_ozon_orders(limit=limit)
    error_rows: list[dict] = []

    for posting in postings:
        try:
            process_posting(posting, dry_run)
        except Exception as e:
            reason = _human_error_from_exception(e)
            posting_number = posting.get("posting_number")

            msg = (
                "❗ Ошибка обработки отправления Ozon\n"
                f"Номер отправления: {posting_number}\n"
                f"Причина: {reason}"
            )

            print("[ORDERS]", msg.replace("\n", " | "))
            try:
                send_telegram_message(msg)
            except Exception:
                pass

            error_rows.extend(_build_error_rows_for_posting(posting, reason))

    _append_order_errors_to_file(error_rows)

if __name__ == "__main__":
    print("Запуск синхронизации заказов Ozon с МойСклад...")
    sync_fbs_orders(dry_run=DRY_RUN_ORDERS, limit=10)
