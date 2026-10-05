-- ============================================================================
-- SALESTORM: Production PostgreSQL Relational Schema
-- Architecture Layer: 04_Database (Canonical Strong-Consistency Persistence)
-- Guarantees: Zero overselling, ACID transactional outbox, and TTL reservation leases.
-- ============================================================================

-- Ensure required extensions exist
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ----------------------------------------------------------------------------
-- 1. INVENTORY TABLE (Source of Truth for Stock Allocation)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory (
    product_id VARCHAR(64) PRIMARY KEY,
    total_quantity INTEGER NOT NULL CHECK (total_quantity >= 0),
    available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0),
    reserved_quantity INTEGER NOT NULL DEFAULT 0 CHECK (reserved_quantity >= 0),
    sold_quantity INTEGER NOT NULL DEFAULT 0 CHECK (sold_quantity >= 0),
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Absolute Mathematical Invariant: Total stock must balance perfectly across states
    CONSTRAINT chk_inventory_balance CHECK (
        available_quantity + reserved_quantity + sold_quantity = total_quantity
    )
);

CREATE INDEX IF NOT EXISTS idx_inventory_available ON inventory (available_quantity);

-- ----------------------------------------------------------------------------
-- 2. INVENTORY RESERVATIONS TABLE (TTL Leases for Flash-Sale Allocation)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory_reservations (
    reservation_id VARCHAR(64) PRIMARY KEY,
    product_id VARCHAR(64) NOT NULL REFERENCES inventory(product_id) ON DELETE RESTRICT,
    customer_id VARCHAR(64) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING' CHECK (
        status IN ('PENDING', 'CONFIRMED', 'EXPIRED', 'CANCELLED')
    ),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for high-throughput TTL expiry sweeps & release of abandoned leases
CREATE INDEX IF NOT EXISTS idx_reservations_status_expires 
    ON inventory_reservations (status, expires_at) 
    WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_reservations_customer_product 
    ON inventory_reservations (customer_id, product_id);

-- ----------------------------------------------------------------------------
-- 3. PAYMENTS TABLE (Idempotent Transaction Audit Log)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments (
    payment_id VARCHAR(64) PRIMARY KEY,
    idempotency_key VARCHAR(128) NOT NULL UNIQUE,
    reservation_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    status VARCHAR(32) NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
    gateway_transaction_id VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_payments_reservation_id ON payments (reservation_id);
CREATE INDEX IF NOT EXISTS idx_payments_customer_id ON payments (customer_id);

-- ----------------------------------------------------------------------------
-- 4. TRANSACTIONAL OUTBOX EVENTS TABLE (Guaranteed Eventual Consistency)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS outbox_events (
    event_id VARCHAR(64) PRIMARY KEY,
    aggregate_type VARCHAR(64) NOT NULL DEFAULT 'PAYMENT',
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING' CHECK (
        status IN ('PENDING', 'PROCESSED', 'FAILED')
    ),
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ NULL
);

-- Partial index for maximum polling efficiency: only scans unacknowledged records
CREATE INDEX IF NOT EXISTS idx_outbox_pending_poll 
    ON outbox_events (created_at ASC) 
    WHERE status = 'PENDING';
