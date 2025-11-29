import os
import requests
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

MS_LOGIN = os.getenv("MS_LOGIN")
MS_PASSWORD = os.getenv("MS_PASSWORD")

BASE_URL = "https://api.moysklad.ru/api/remap/1.2"

# Создаём глобальную сессию для всех запросов
session = requests.Session()
session.auth = (MS_LOGIN, MS_PASSWORD)
session.headers.update({
    "Accept-Encoding": "gzip",
    "User-Agent": "MS-Ozon-Integration",
})


def get_stock_all(limit: int = 1000, offset: int = 0, store_id: str | None = None) -> dict:
    """
    Запрос отчёта /report/stock/all.
    Если указан store_id — возвращаем остатки только по этому складу.
    """
    url = f"{BASE_URL}/report/stock/all"

    params: dict[str, object] = {
        "limit": limit,
        "offset": offset,
    }

    if store_id:
        # Если store_id — GUID, то собираем href
        if store_id.startswith("http"):
            stock_store_href = store_id
        else:
            stock_store_href = f"{BASE_URL}/entity/store/{store_id}"

        params["stockStore"] = stock_store_href

    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def find_product_by_article(article: str) -> dict | None:
    """
    Поиск товара по артикулу.
    """
    url = f"{BASE_URL}/entity/product"
    params = {"filter": f"article={article}"}

    resp = session.get(url, params=params, timeout=20)
    resp.raise_for_status()
    rows = resp.json().get("rows", [])
    return rows[0] if rows else None


def get_store_href(store_id: str) -> str:
    """
    Полный href склада.
    """
    if store_id.startswith("http"):
        return store_id
    return f"{BASE_URL}/entity/store/{store_id}"
