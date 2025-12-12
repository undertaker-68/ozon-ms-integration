from ozon_fbo_client import OzonFboClient
from dotenv import load_dotenv
import os

load_dotenv()

def run(acc: str, cid: str, key: str):
    if not cid or not key:
        print(f"[{acc}] нет ключей")
        return
    c = OzonFboClient(cid, key, account_name=acc)
    orders = c.get_supply_orders(limit=100, days_back=120)  # окно пошире для поиска
    print(f"\n[{acc}] всего заявок: {len(orders)}")
    for o in orders[:100]:
        print(
            f"id={o.get('order_id')}, number={o.get('order_number')}, "
            f"created={o.get('created_date')}, state={o.get('state')}"
        )

run("ozon1", os.getenv("OZON_CLIENT_ID"), os.getenv("OZON_API_KEY"))
run("ozon2", os.getenv("OZON2_CLIENT_ID"), os.getenv("OZON2_API_KEY"))
