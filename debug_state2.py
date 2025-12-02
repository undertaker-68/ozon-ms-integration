from ozon_client2 import HEADERS, OZON_API_URL
import requests

# Сюда впиши offer_id товаров, которые есть во втором кабинете,
# но они НЕ попадают в CSV (например, "VW12" и другие).
TEST_IDS = [
    "VW12",
    # добавь сюда ещё 2-3 артикула из второго кабинета
]

url = f"{OZON_API_URL}/v3/product/info/list"

body = {
    "offer_id": TEST_IDS,
}

print("=== REQUEST ===")
print(body)

resp = requests.post(url, json=body, headers=HEADERS)
print("=== RESPONSE STATUS ===")
print(resp.status_code)

print("=== RESPONSE TEXT ===")
print(resp.text)
