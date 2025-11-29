import os
import requests
from dotenv import load_dotenv

load_dotenv()

MS_LOGIN = os.getenv("MS_LOGIN")
MS_PASSWORD = os.getenv("MS_PASSWORD")

BASE_URL = "https://api.moysklad.ru/api/remap/1.2"

# Одна общая сессия для всех запросов в МС
session = requests.Session()
session.auth = (MS_LOGIN, MS_PASSWORD)
session.headers.update({
    "Accept-Encoding": "gzip",
    "User-Agent": "MS-Ozon-Integration",
})


def get_stock_all(limit: int = 100, offset: int = 0, store_id: str | None = None) -> dict:
    """
    Получаем остатки через /entity/assortment.

    ВАЖНО:
      - Работает по одному складу.
      - Фильтр по складу делаем через filter=stockStore=<href склада>.
      - stockMode=all, чтобы видеть и нули, и отрицательные остатки.

    Возвращает сырой ответ JSON от МойСклад (dict).
    """
    url = f"{BASE_URL}/entity/assortment"

    params: dict[str, object] = {
        "limit": limit,
        "offset": offset,
        "stockMode": "all",
    }

    # Если склад не передан явно — используем MS_OZON_STORE_ID из .env
    if store_id is None:
        if not MS_OZON_STORE_ID:
            raise RuntimeError("Не задан MS_OZON_STORE_ID в .env и не передан store_id в get_stock_all")
        store_id = MS_OZON_STORE_ID

    # Собираем href склада
    if str(store_id).startswith("http"):
        stock_store_href = store_id
    else:
        stock_store_href = f"{BASE_URL}/entity/store/{store_id}"

    # 🔴 ГЛАВНОЕ ИСПРАВЛЕНИЕ:
    # Вместо отдельного параметра stockStore используем filter=stockStore=<href>
    params["filter"] = f"stockStore={stock_store_href}"

    data = _ms_get(url, params=params)
    return data


def find_product_by_article(article: str) -> dict | None:
    """
    Простейший поиск товара по артикулу (entity/product).
    Если нужно, потом можно расширить (модификации, комплекты и т.д.).
    """
    url = f"{BASE_URL}/entity/product"
    params = {"filter": f"article={article}"}

    resp = session.get(url, params=params, timeout=20)
    resp.raise_for_status()
    rows = resp.json().get("rows", [])
    return rows[0] if rows else None


def get_store_href(store_id: str) -> str:
    """
    Собрать href склада.
    """
    if store_id.startswith("http"):
        return store_id
    return f"{BASE_URL}/entity/store/{store_id}"
