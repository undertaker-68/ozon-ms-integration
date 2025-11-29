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


def get_stock_all(limit: int = 1000, offset: int = 0, store_id: str | None = None) -> dict:
    """
    Получить остатки по складу через /entity/assortment.

    ВАЖНО:
    - /report/stock/all -> поле stock = общий остаток по организации.
    - /entity/assortment + stockStore -> поле stock = остаток по указанному складу.

    Поэтому здесь мы используем именно assortment, а не report/stock/all.
    """
    url = f"{BASE_URL}/entity/assortment"

    params: dict[str, object] = {
        "limit": limit,
        "offset": offset,
        # Показывать ВСЕ товары, включая с нулевыми и отрицательными остатками
        "stockMode": "all",
    }

    if store_id:
        # Если передали полный href склада — используем как есть
        if store_id.startswith("http"):
            stock_store_href = store_id
        else:
            stock_store_href = f"{BASE_URL}/entity/store/{store_id}"

        # Этот параметр как раз фильтрует остатки по складу
        params["stockStore"] = stock_store_href

    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


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
