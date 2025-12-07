# debug_ms_stores.py
from pprint import pprint

from ms_client import BASE_URL, _ms_get  # _ms_get уже есть в ms_client.py

def main():
    url = f"{BASE_URL}/entity/store"
    data = _ms_get(url, params={"limit": 100})
    rows = data.get("rows") or []

    print("=== СПИСОК СКЛАДОВ ===")
    for row in rows:
        print(f"{row.get('id')}  |  {row.get('name')}")

    print("\n=== ПОЛНЫЕ ДАННЫЕ (если нужно смотреть детали) ===")
    pprint(rows)

if __name__ == "__main__":
    main()
