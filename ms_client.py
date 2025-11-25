import base64
import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

MS_LOGIN = os.getenv("MS_LOGIN")
MS_PASSWORD = os.getenv("MS_PASSWORD")

if not MS_LOGIN or not MS_PASSWORD:
    raise RuntimeError("Не заданы MS_LOGIN / MS_PASSWORD в .env")

# Basic auth в заголовке Authorization
AUTH = base64.b64encode(f"{MS_LOGIN}:{MS_PASSWORD}".encode("utf-8")).decode("utf-8")

HEADERS = {
    "Authorization": f"Basic {AUTH}",
    "Accept": "application/json;charset=utf-8",
    "Accept-Encoding": "gzip",
}

BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
MS_BASE_URL = BASE_URL


def _ms_get(url: str, params: dict | None = None) -> dict:
    """
    Вспомогательный GET-запрос к МойСклад с логированием.
    """
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    print(f"Запрос к МС: {r.url}")
    print("Статус:", r.status_code)
    if r.status_code >= 400:
        print("Ответ МС:", r.text[:2000])
    r.raise_for_status()
    return r.json()


def get_products(limit: int = 10, offset: int = 0) -> dict:
    """
    Получить список товаров из МойСклад.
    """
    url = f"{BASE_URL}/entity/product"
    params = {"limit": limit, "offset": offset}
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()


def get_stock_all(limit: int = 100, offset: int = 0) -> dict:
    """
    Отчет по остаткам: /report/stock/all
    Возвращает словарь, в котором нас интересует ключ 'rows'.
    """
    url = f"{BASE_URL}/report/stock/all"
    params = {"limit": limit, "offset": offset}
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()


# ==========================
# НОРМАЛИЗАЦИЯ АРТИКУЛА
# ==========================

def _normalize_article(s: str | None) -> str:
    """
    Нормализация артикула:
      - убираем пробелы по краям;
      - заменяем визуально одинаковые русские буквы на латиницу;
      - приводим к верхнему регистру.

    Это позволяет сравнивать 'E90' и 'Е90' как одно и то же.
    """
    if not s:
        return ""
    s = s.strip()

    # Русские → латинские (визуально похожие)
    repl_map = {
        "А": "A", "а": "a",
        "В": "B", "в": "b",
        "С": "C", "с": "c",
        "Е": "E", "е": "e",
        "К": "K", "к": "k",
        "М": "M", "м": "m",
        "Н": "H", "н": "h",
        "О": "O", "о": "o",
        "Р": "P", "р": "p",
        "Т": "T", "т": "t",
        "Х": "X", "х": "x",
    }

    s_norm = []
    for ch in s:
        s_norm.append(repl_map.get(ch, ch))

    return "".join(s_norm).upper()


def _articles_equal(a: str | None, b: str | None) -> bool:
    """
    Сравнение артикулов с учётом нормализации.
    """
    return _normalize_article(a) == _normalize_article(b)


# ==========================
# ПОИСК ПО product / bundle
# ==========================

def _find_in_entity_by_article(entity_url: str, article: str) -> dict | None:
    """
    Пытается найти товар в указанной сущности (product или bundle)
    по артикулу article.

    Логика:
      1) /entity/... ?filter=article=...
         -> берем только те, у кого article совпадает после нормализации.
      2) /entity/... ?search=...
         -> также фильтруем по article (с нормализацией).
    Учитываем ТОЛЬКО поле article, code намеренно игнорируем.
    """
    target = article

    # 1. Точный фильтр по article
    try:
        params = {"filter": f"article={target}"}
        data = _ms_get(entity_url, params=params)
        rows = data.get("rows", [])
        if rows:
            exact = [
                r for r in rows
                if _articles_equal(r.get("article"), target)
            ]
            if exact:
                r0 = exact[0]
                print(
                    f"Найден в {entity_url} по filter article={target}: "
                    f"{r0.get('name')} (article={r0.get('article')})"
                )
                return r0
            else:
                print(
                    f"{entity_url}: filter article={target} вернул товары, "
                    f"но article не совпал после нормализации"
                )
        else:
            print(f"{entity_url}: filter article={target} вернул 0 товаров")
    except Exception as e:
        print(f"Ошибка поиска в {entity_url} по filter article={target}: {e!r}")

    # 2. Поиск через search
    try:
        params = {"search": target}
        data = _ms_get(entity_url, params=params)
        rows = data.get("rows", [])
        if rows:
            exact = [
                r for r in rows
                if _articles_equal(r.get("article"), target)
            ]
            if exact:
                r0 = exact[0]
                print(
                    f"Найден в {entity_url} по search={target} с точным article: "
                    f"{r0.get('name')} (article={r0.get('article')})"
                )
                return r0
            else:
                print(
                    f"{entity_url}: search={target} вернул товары, но article не совпал "
                    f"после нормализации с {target}"
                )
        else:
            print(f"{entity_url}: search={target} вернул 0 товаров")
    except Exception as e:
        print(f"Ошибка поиска в {entity_url} по search={target}: {e!r}")

    return None


def find_product_by_article(article: str) -> dict | None:
    """
    Ищет ассортименты в МойСклад по артикулу из Ozon.

    ВАЖНО:
      - Ищем сначала среди обычных товаров (/entity/product),
        потом среди комплектов (/entity/bundle).
      - Учитываем ТОЛЬКО поле article.
      - Сравниваем артикулы с нормализацией раскладки (E/Е, С/С и т.д.).
      - code намеренно игнорируем.
    """
    product_url = f"{MS_BASE_URL}/entity/product"
    bundle_url = f"{MS_BASE_URL}/entity/bundle"

    # 1. Обычные товары
    product = _find_in_entity_by_article(product_url, article)
    if product is not None:
        return product

    # 2. Комплекты (bundle)
    bundle = _find_in_entity_by_article(bundle_url, article)
    if bundle is not None:
        return bundle

    print(
        f"Товар/комплект в МойСклад не найден ни в product, ни в bundle "
        f"по article={article} (с учётом нормализации)"
    )
    return None


# ==========================
# ЗАКАЗЫ ПОКУПАТЕЛЯ (customerorder)
# ==========================

def create_customer_order(payload: dict) -> dict:
    url = f"{BASE_URL}/entity/customerorder"
    print("=== Запрос в МойСклад /entity/customerorder ===")
    print("URL:", url)
    print("Тело запроса (фрагмент):")
    try:
        print(json.dumps(payload, ensure_ascii=False, indent=2)[:2000])
    except Exception:
        print(str(payload)[:2000])
    print("=== /Запрос ===")

    r = requests.post(url, json=payload, headers=HEADERS, timeout=30)

    print("=== Ответ МойСклад /entity/customerorder ===")
    print("HTTP status:", r.status_code)
    print("Тело ответа:")
    print(r.text)
    print("=== /Ответ ===")

    r.raise_for_status()
    return r.json()


def find_customer_order_by_name(name: str) -> dict | None:
    """
    Поиск заказа покупателя по полю name.
    """
    url = f"{BASE_URL}/entity/customerorder"
    params = {"filter": f"name={name}", "limit": 1}
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    data = r.json()
    rows = data.get("rows", [])
    return rows[0] if rows else None


def update_customer_order_state(order_meta_href: str, state_meta_href: str) -> dict:
    """
    Смена статуса (state) заказа покупателя.
    order_meta_href — meta.href самого заказа (из поля meta заказа).
    state_meta_href — meta.href нужного статуса (надо взять из /metadata).
    """
    r = requests.get(order_meta_href, headers=HEADERS)
    r.raise_for_status()
    order = r.json()

    order["state"] = {
        "meta": {
            "href": state_meta_href
        }
    }

    r_put = requests.put(order_meta_href, headers=HEADERS, json=order)
    r_put.raise_for_status()
    return r_put.json()


def clear_reserve_for_order(order_meta_href: str) -> dict:
    """
    Снять резерв по всем позициям заказа.
    """
    r = requests.get(order_meta_href, headers=HEADERS)
    r.raise_for_status()
    order = r.json()

    positions = order.get("positions", [])
    for pos in positions:
        # reserve может отсутствовать — тогда считаем, что 0
        pos["reserve"] = 0

    order["positions"] = positions

    r_put = requests.put(order_meta_href, headers=HEADERS, json=order)
    r_put.raise_for_status()
    return r_put.json()


def create_demand_from_order(order_meta_href: str) -> dict:
    """
    Создать документ 'Отгрузка' (demand) на основе заказа покупателя.
    Берём все позиции из заказа, копируем quantity и assortment.meta.
    """
    # 1. Получаем заказ целиком
    r = requests.get(order_meta_href, headers=HEADERS)
    r.raise_for_status()
    order = r.json()

    demand_payload = {
        "customerOrder": {
            "meta": order["meta"]
        },
        "organization": order.get("organization"),
        "agent": order.get("agent"),
        "store": order.get("store"),
        "positions": [],
    }

    for pos in order.get("positions", []):
        demand_payload["positions"].append({
            "quantity": pos.get("quantity", 0),
            "assortment": pos.get("assortment"),
        })

    url = f"{BASE_URL}/entity/demand"
    r_post = requests.post(url, headers=HEADERS, json=demand_payload)
    r_post.raise_for_status()
    return r_post.json()


if __name__ == "__main__":
    print("=== Тест товаров ===")
    products = get_products(limit=1)
    print(products)

    print("\n=== Тест отчета по остаткам ===")
    stock = get_stock_all(limit=5)
    print(stock)
