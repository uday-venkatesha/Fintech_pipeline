-- ─────────────────────────────────────────────────────────────────────────
--  Source DB initialisation
--  Runs once when postgres-source container starts for the first time.
--  Creates the transactions table our generator will write to.
-- ─────────────────────────────────────────────────────────────────────────

-- Extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── Main transactions table ──────────────────────────────────────────────
-- This simulates what a real payment application would write to Postgres.
-- Key design decisions:
--   - id: UUID avoids hotspot inserts vs sequential int PKs at scale
--   - created_at: immutable, when the row was first written (never changes)
--   - updated_at: changes on any status update (pending → settled → failed)
--     This is our WATERMARK COLUMN for incremental extraction.
--   - status: transactions go through a lifecycle — important for our
--     watermark strategy because a row can be re-updated after creation.

CREATE TABLE IF NOT EXISTS transactions (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID        NOT NULL,
    merchant_id     UUID        NOT NULL,
    merchant_name   VARCHAR(120) NOT NULL,
    merchant_category VARCHAR(50) NOT NULL,
    amount          NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    currency        CHAR(3)     NOT NULL DEFAULT 'USD',
    status          VARCHAR(20) NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending','processing','settled','failed','refunded')),
    channel         VARCHAR(20) NOT NULL
                                CHECK (channel IN ('online','in_store','mobile','atm')),
    card_last_four  CHAR(4)     NOT NULL,
    country_code    CHAR(2)     NOT NULL DEFAULT 'US',
    error_code      VARCHAR(30),          -- NULL unless status = failed
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Indexes ──────────────────────────────────────────────────────────────
-- updated_at index is CRITICAL — this is what makes our incremental
-- extract fast. Without it, every extract is a full table scan.
CREATE INDEX IF NOT EXISTS idx_transactions_updated_at
    ON transactions (updated_at ASC);

-- Composite index for common analytical queries (by user, time)
CREATE INDEX IF NOT EXISTS idx_transactions_user_updated
    ON transactions (user_id, updated_at DESC);

-- Status index — pipeline uses this to find rows that need status updates
CREATE INDEX IF NOT EXISTS idx_transactions_status
    ON transactions (status) WHERE status IN ('pending', 'processing');


-- ── Watermark tracking table ─────────────────────────────────────────────
-- Our Airflow DAG will write its high-water mark here after each
-- successful run. This is the "checkpoint" pattern.
-- If a run fails, the watermark is NOT updated — so the next run
-- will re-process the same window. This gives us at-least-once semantics.

CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    pipeline_name   VARCHAR(100) PRIMARY KEY,
    last_updated_at TIMESTAMPTZ  NOT NULL,
    last_run_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    rows_processed  BIGINT       NOT NULL DEFAULT 0,
    run_status      VARCHAR(20)  NOT NULL DEFAULT 'success'
);

-- Seed the watermark at epoch so first run picks up all existing data
INSERT INTO pipeline_watermarks (pipeline_name, last_updated_at, rows_processed)
VALUES ('hourly_batch_pipeline', '1970-01-01 00:00:00+00', 0)
ON CONFLICT (pipeline_name) DO NOTHING;


-- ── Status update trigger ─────────────────────────────────────────────────
-- Automatically bumps updated_at on every UPDATE.
-- Without this, a status change from pending → settled would not
-- update the watermark column, and our incremental extract would MISS it.
-- This is a common production bug.

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_transactions_updated_at
    BEFORE UPDATE ON transactions
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();


-- ── Verification view ────────────────────────────────────────────────────
-- Quick sanity check during development
CREATE OR REPLACE VIEW v_transaction_summary AS
SELECT
    status,
    merchant_category,
    COUNT(*)                            AS tx_count,
    SUM(amount)                         AS total_amount,
    AVG(amount)                         AS avg_amount,
    MIN(created_at)                     AS oldest,
    MAX(updated_at)                     AS newest
FROM transactions
GROUP BY status, merchant_category
ORDER BY tx_count DESC;

-- ── Done ─────────────────────────────────────────────────────────────────
DO $$ BEGIN
    RAISE NOTICE 'Source DB initialised. Tables: transactions, pipeline_watermarks';
END $$;