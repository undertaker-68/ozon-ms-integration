import time
import traceback
import os

from dotenv import load_dotenv

from sync_orders import sync_fbs_orders
from sync_stock import main as sync_stock_main  # <-- ВАЖНО: импортируем main

load_dotenv()

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
DRY_RUN_ORDERS = os.getenv("DRY_RUN_ORDERS", "true").lower() == "true"

ORDERS_INTERVAL = 5 * 60         # заказы каждые 5 минут
STOCK_INTERVAL = 8 * 60 * 60     # остатки каждые 8 часов

last_orders_run = 0
last_stock_run = 0

print("=== Автоматический режим интеграции Ozon ↔ МойСклад запущен ===")
print(f"Обновление заказов: каждые 5 минут (DRY_RUN_ORDERS={DRY_RUN_ORDERS})")
print(f"Обновление остатков: каждые 8 часов (DRY_RUN={DRY_RUN})")
print("===============================================================")

while True:
    now = time.time()

    # --- Обновление заказов каждые 5 минут ---
    if now - last_orders_run >= ORDERS_INTERVAL:
        print("\n[RUN] Обновление заказов...")
        try:
            # limit можешь подправить под себя
            sync_fbs_orders(dry_run=DRY_RUN_ORDERS, limit=50)
        except Exception:
            print("[ERROR] Ошибка при обновлении заказов:")
            print(traceback.format_exc())
        else:
            print("[DONE] Заказы обновлены.")
        last_orders_run = now

    # --- Обновление остатков каждые 8 часов ---
    if now - last_stock_run >= STOCK_INTERVAL:
        print("\n[RUN] Обновление остатков...")
        try:
            sync_stock_main(dry_run=DRY_RUN)
        except Exception:
            print("[ERROR] Ошибка при обновлении остатков:")
            print(traceback.format_exc())
        else:
            print("[DONE] Остатки обновлены.")
        last_stock_run = now

    # Небольшая пауза, чтобы не греть CPU
    time.sleep(10)
