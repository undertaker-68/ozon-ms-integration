import time
import traceback
from sync_orders import sync_fbs_orders
from sync_stock import sync_stocks
from dotenv import load_dotenv
import os

load_dotenv()

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
DRY_RUN_ORDERS = os.getenv("DRY_RUN_ORDERS", "true").lower() == "true"

ORDERS_INTERVAL = 5 * 60          # каждые 5 минут
STOCK_INTERVAL = 8 * 60 * 60      # каждые 8 часов

last_orders_run = 0
last_stock_run = 0

print("=== Автоматический режим интеграции Ozon ↔ МойСклад запущен ===")
print(f"Обновление заказов: каждые 5 минут (dry={DRY_RUN_ORDERS})")
print(f"Обновление остатков: каждые 8 часов (dry={DRY_RUN})")
print("===============================================================")

while True:
    now = time.time()

    # --- Обновление заказов ---
    if now - last_orders_run >= ORDERS_INTERVAL:
        print("\n[RUN] Обновление заказов...")
        try:
            sync_fbs_orders(dry_run=DRY_RUN_ORDERS, limit=50)
        except Exception as e:
            print("[ERROR] Ошибка при обновлении заказов:")
            print(traceback.format_exc())
        last_orders_run = now
        print("[DONE] Заказы обновлены.")

    # --- Обновление остатков ---
    if now - last_stock_run >= STOCK_INTERVAL:
        print("\n[RUN] Обновление остатков...")
        try:
            sync_stocks(dry_run=DRY_RUN)
        except Exception as e:
            print("[ERROR] Ошибка при обновлении остатков:")
            print(traceback.format_exc())
        last_stock_run = now
        print("[DONE] Остатки обновлены.")

    # пауза 10 секунд, чтобы не грузить процессор
    time.sleep(10)
