# debug_ms_00519.py
from pprint import pprint

from ms_client import get_stock_all, MS_OZON_STORE_ID

def main():
    # читаем первую тысячу строк по складу Ozon
    data = get_stock_all(limit=1000, offset=0, store_id=MS_OZON_STORE_ID)
    rows = data.get("rows") or []

    found = False

    for row in rows:
        code = row.get("code")
        article = row.get("article")
        if code == "00519" or article == "00519":
            print("=== НАЙДЕНА СТРОКА ПО 00519 ===")
            pprint(row)
            found = True
            break

    if not found:
        print("Строка по 00519 в /entity/assortment не найдена по складу Ozon")

if __name__ == "__main__":
    main()
