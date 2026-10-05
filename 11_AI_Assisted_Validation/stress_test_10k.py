import asyncio
import uuid
import time
from collections import Counter
import fakeredis.aioredis as fakeredis

TOTAL_REQUESTS = 10_000
INITIAL_STOCK = 100
ITEM_ID = "flash_deal_macbook_pro"

RESERVE_STOCK_LUA = """
local existing_reservation = redis.call("GET", KEYS[3])
if existing_reservation then
    return -1
end

local current_stock = redis.call("GET", KEYS[1])
if not current_stock then
    return -2
end

current_stock = tonumber(current_stock)
local qty = tonumber(ARGV[1])

if current_stock < qty then
    return 0
end

redis.call("DECRBY", KEYS[1], qty)

redis.call("HMSET", KEYS[2],
    "reservation_id", ARGV[2],
    "user_id", ARGV[4],
    "qty", qty,
    "status", "PENDING_PAYMENT"
)
redis.call("EXPIRE", KEYS[2], tonumber(ARGV[3]))
redis.call("SET", KEYS[3], ARGV[2], "EX", tonumber(ARGV[3]))

return 1
"""

async def execute_purchase(
    client,
    script_sha: str,
    user_index: int,
    start_gate: asyncio.Event,
    results: list
):
    user_id = f"user_{user_index:05d}"
    idempotency_key = f"key_{user_index:05d}_{uuid.uuid4().hex[:6]}"
    reservation_id = f"res_{uuid.uuid4().hex[:8]}"

    stock_key = f"stock:{ITEM_ID}"
    reservation_key = f"reservation:{reservation_id}"
    idemp_key = f"idemp:{user_id}:{idempotency_key}"

    keys = [stock_key, reservation_key, idemp_key]
    args = [1, reservation_id, 300, user_id]

    await start_gate.wait()

    try:
        status_code = await client.evalsha(script_sha, len(keys), *keys, *args)
        results.append(status_code)
    except Exception as e:
        results.append(f"ERROR: {str(e)}")

async def run_simulation():
    # Enable sufficient connection pool capacity for 10,000 simultaneous coroutines
    client = fakeredis.FakeRedis(decode_responses=True, max_connections=TOTAL_REQUESTS)
    stock_key = f"stock:{ITEM_ID}"
    await client.set(stock_key, INITIAL_STOCK)
    
    script_sha = await client.script_load(RESERVE_STOCK_LUA)

    print("================================================================")
    print("SALESTORM: 10,000 CONCURRENT REQUESTS STRESS HARNESS")
    print(f"Target Item: {ITEM_ID}")
    print(f"Initial Stock: {INITIAL_STOCK}")
    print(f"Concurrent Callers: {TOTAL_REQUESTS}")
    print("================================================================")

    start_gate = asyncio.Event()
    results = []

    tasks = [
        asyncio.create_task(
            execute_purchase(client, script_sha, i, start_gate, results)
        )
        for i in range(TOTAL_REQUESTS)
    ]

    print(f"-> {TOTAL_REQUESTS} coroutines ready and waiting on gate...")
    await asyncio.sleep(0.5)

    t0 = time.perf_counter()
    start_gate.set()
    await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - t0

    final_stock = int(await client.get(stock_key))
    counts = Counter(results)

    success_count = counts.get(1, 0)
    out_of_stock_count = counts.get(0, 0)
    errors_count = sum(v for k, v in counts.items() if isinstance(k, str) and k.startswith("ERROR"))

    print("\n--- RESULTS & INVARIANT VERIFICATION ---")
    print(f"Total Completed Requests : {len(results)}")
    print(f"Execution Duration       : {elapsed:.3f} seconds")
    print(f"Throughput               : {TOTAL_REQUESTS / elapsed:,.0f} req/sec")
    print(f"Confirmed Reservations   : {success_count}  (Status = 1)")
    print(f"Rejected (Out of Stock)  : {out_of_stock_count} (Status = 0)")
    print(f"Errors / Collisions      : {errors_count}")
    print(f"Final Available Stock    : {final_stock}")

    assert success_count == INITIAL_STOCK, f"VIOLATION: Expected {INITIAL_STOCK} reservations, got {success_count}"
    assert out_of_stock_count == (TOTAL_REQUESTS - INITIAL_STOCK), "VIOLATION: Rejection count mismatch"
    assert final_stock == 0, f"VIOLATION: Stock dropped below zero or failed to exhaust! final_stock={final_stock}"
    assert final_stock >= 0, "FATAL OVERSELL: Negative stock detected!"

    print("\n[PASSED] Invariant held: Zero oversell. Exactly 100 units reserved across 10,000 parallel requests.")
    await client.aclose()

if __name__ == "__main__":
    asyncio.run(run_simulation())
