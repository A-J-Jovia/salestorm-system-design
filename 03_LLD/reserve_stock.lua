-- KEYS:
-- KEYS[1] = stock_key         (e.g., "stock:item_1001")
-- KEYS[2] = reservation_key   (e.g., "reservation:res_98fbc12a")
-- KEYS[3] = idempotency_key   (e.g., "idemp:user_42:sale_item_1001")

-- ARGV:
-- ARGV[1] = requested_qty     (e.g., "1")
-- ARGV[2] = reservation_id    (e.g., "res_98fbc12a")
-- ARGV[3] = ttl_seconds       (e.g., "300")
-- ARGV[4] = user_id           (e.g., "user_42")

-- Return Codes:
--  1 : SUCCESS (Stock reserved)
--  0 : OUT_OF_STOCK (Insufficient stock)
-- -1 : IDEMPOTENT_HIT (Request already processed)
-- -2 : INVALID_STOCK_KEY (Key does not exist)

-- 1. Idempotency Check
local existing_reservation = redis.call("GET", KEYS[3])
if existing_reservation then
    return -1
end

-- 2. Validate Stock Key
local current_stock = redis.call("GET", KEYS[1])
if not current_stock then
    return -2
end

current_stock = tonumber(current_stock)
local qty = tonumber(ARGV[1])

-- 3. Invariant Evaluation: Never allow stock to drop below 0
if current_stock < qty then
    return 0
end

-- 4. Atomic Mutation
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
