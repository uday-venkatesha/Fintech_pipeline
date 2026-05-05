"""
Transaction Generator
─────────────────────
Continuously inserts synthetic financial transactions into the source
Postgres DB. Simulates a live payment application.

Production concepts demonstrated:
  - Realistic data distribution (not uniform random)
  - Transaction lifecycle updates (pending → settled/failed)
  - Configurable insert rate
  - Graceful shutdown on SIGTERM (important in Docker)
  - Structured logging
  - Connection pooling via psycopg3

Run: python generate_transactions.py
     SOURCE_DB_CONN env var must be set.
"""

import os
import sys
import time
import random
import signal
import logging
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import psycopg
from psycopg.rows import dict_row
from faker import Faker

# ── Logging setup ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GENERATOR] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
DB_CONN       = os.environ["SOURCE_DB_CONN"]
INSERT_RATE   = int(os.environ.get("INSERT_RATE", "3"))    # new txs per second
UPDATE_RATE   = int(os.environ.get("UPDATE_RATE", "2"))    # status updates per second
BATCH_SIZE    = int(os.environ.get("BATCH_SIZE", "10"))    # rows per INSERT call
STARTUP_DELAY = int(os.environ.get("STARTUP_DELAY", "5")) # seconds to wait for DB

fake = Faker()
Faker.seed(42)

# ── Realistic data pools ───────────────────────────────────────────────────
# Weighted distributions mirror real transaction patterns.
# Most transactions are small; a few are large (power law).

MERCHANT_POOL = [
    ("Amazon",          "retail",        (5,    500)),
    ("Walmart",         "retail",        (10,   200)),
    ("Target",          "retail",        (15,   300)),
    ("Starbucks",       "food_beverage", (3,    25)),
    ("McDonald's",      "food_beverage", (5,    30)),
    ("Chipotle",        "food_beverage", (8,    20)),
    ("Shell",           "fuel",          (20,   120)),
    ("BP",              "fuel",          (25,   110)),
    ("Delta Airlines",  "travel",        (150,  1200)),
    ("Marriott",        "travel",        (80,   600)),
    ("Netflix",         "subscription",  (15,   25)),
    ("Spotify",         "subscription",  (10,   16)),
    ("CVS Pharmacy",    "healthcare",    (5,    200)),
    ("Walgreens",       "healthcare",    (8,    180)),
    ("Home Depot",      "home_garden",   (20,   500)),
    ("Apple Store",     "electronics",   (50,   2000)),
    ("Whole Foods",     "grocery",       (20,   300)),
    ("Kroger",          "grocery",       (15,   250)),
    ("Planet Fitness",  "fitness",       (10,   50)),
    ("Chase ATM",       "atm",           (20,   500)),
]

# Status transition probabilities (realistic settlement rates)
STATUS_TRANSITIONS = {
    "pending":    [("processing", 0.85), ("failed", 0.15)],
    "processing": [("settled",    0.92), ("failed", 0.08)],
}

CHANNELS   = ["online", "online", "online", "mobile", "in_store", "in_store", "atm"]
CURRENCIES = ["USD"] * 90 + ["EUR"] * 5 + ["GBP"] * 3 + ["CAD"] * 2
COUNTRIES  = ["US"] * 80 + ["CA"] * 8 + ["GB"] * 5 + ["DE"] * 3 + ["AU"] * 2 + ["MX"] * 2

ERROR_CODES = [
    "INSUFFICIENT_FUNDS",
    "CARD_DECLINED",
    "INVALID_CVV",
    "EXPIRED_CARD",
    "FRAUD_SUSPECTED",
    "NETWORK_ERROR",
]

# Pre-generate a pool of user_ids and merchant_ids.
# Real apps have a fixed set of users — not a new UUID every row.
USER_POOL     = [str(uuid.uuid4()) for _ in range(500)]
MERCHANT_POOL_IDS = {name: str(uuid.uuid4()) for name, _, _ in MERCHANT_POOL}


# ── Row generation ─────────────────────────────────────────────────────────

def generate_transaction() -> dict:
    """Generate a single realistic transaction row."""
    merchant_name, category, (amt_min, amt_max) = random.choice(MERCHANT_POOL)
    channel = random.choice(CHANNELS)

    # Skew amounts: most are small, occasional large purchase
    # Using triangular distribution for more realistic bell curve
    amount = round(
        random.triangular(amt_min, amt_max, amt_min + (amt_max - amt_min) * 0.25),
        2
    )

    return {
        "id":                 str(uuid.uuid4()),
        "user_id":            random.choice(USER_POOL),
        "merchant_id":        MERCHANT_POOL_IDS[merchant_name],
        "merchant_name":      merchant_name,
        "merchant_category":  category,
        "amount":             Decimal(str(amount)),
        "currency":           random.choice(CURRENCIES),
        "status":             "pending",
        "channel":            channel,
        "card_last_four":     fake.numerify("####"),
        "country_code":       random.choice(COUNTRIES),
        "error_code":         None,
    }


def pick_status_update(current_status: str) -> tuple[str, str | None]:
    """
    Given current status, return (new_status, error_code).
    Returns (None, None) if no transition should happen.
    """
    if current_status not in STATUS_TRANSITIONS:
        return None, None

    transitions = STATUS_TRANSITIONS[current_status]
    rand = random.random()
    cumulative = 0.0
    for new_status, prob in transitions:
        cumulative += prob
        if rand < cumulative:
            error = random.choice(ERROR_CODES) if new_status == "failed" else None
            return new_status, error

    return None, None


# ── Database operations ────────────────────────────────────────────────────

INSERT_SQL = """
    INSERT INTO transactions (
        id, user_id, merchant_id, merchant_name, merchant_category,
        amount, currency, status, channel, card_last_four,
        country_code, error_code
    ) VALUES (
        %(id)s, %(user_id)s, %(merchant_id)s, %(merchant_name)s,
        %(merchant_category)s, %(amount)s, %(currency)s, %(status)s,
        %(channel)s, %(card_last_four)s, %(country_code)s, %(error_code)s
    )
"""

UPDATE_SQL = """
    UPDATE transactions
    SET    status = %(new_status)s,
           error_code = %(error_code)s
           -- updated_at is bumped automatically by the trigger
    WHERE  id = %(id)s
      AND  status = %(current_status)s
"""

FETCH_PENDING_SQL = """
    SELECT id, status
    FROM   transactions
    WHERE  status IN ('pending', 'processing')
    ORDER  BY created_at ASC
    LIMIT  %(limit)s
"""


def insert_batch(conn, rows: list[dict]) -> int:
    """Insert a batch of new transactions. Returns count inserted."""
    with conn.cursor() as cur:
        cur.executemany(INSERT_SQL, rows)
    conn.commit()
    return len(rows)


def update_statuses(conn, limit: int = 20) -> int:
    """
    Advance a batch of pending/processing transactions to next status.
    This is what makes updated_at change on existing rows —
    the interesting case for our incremental watermark strategy.
    """
    updated = 0
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(FETCH_PENDING_SQL, {"limit": limit})
        rows = cur.fetchall()

    if not rows:
        return 0

    updates = []
    for row in rows:
        new_status, error_code = pick_status_update(row["status"])
        if new_status:
            updates.append({
                "id":             row["id"],
                "current_status": row["status"],
                "new_status":     new_status,
                "error_code":     error_code,
            })

    if updates:
        with conn.cursor() as cur:
            cur.executemany(UPDATE_SQL, updates)
        conn.commit()
        updated = len(updates)

    return updated


# ── Main loop ──────────────────────────────────────────────────────────────

# Graceful shutdown flag — set by SIGTERM handler
_RUNNING = True

def handle_sigterm(signum, frame):
    global _RUNNING
    log.info("SIGTERM received — shutting down gracefully...")
    _RUNNING = False

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT,  handle_sigterm)


def wait_for_db(dsn: str, retries: int = 20, delay: float = 3.0):
    """Retry connecting to DB before starting. DB may not be ready immediately."""
    for attempt in range(1, retries + 1):
        try:
            with psycopg.connect(dsn) as conn:
                conn.execute("SELECT 1")
            log.info("Connected to source DB ✓")
            return
        except Exception as e:
            log.warning(f"DB not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    log.error("Could not connect to DB after all retries. Exiting.")
    sys.exit(1)


def main():
    log.info(f"Starting transaction generator | rate={INSERT_RATE} tx/s | batch={BATCH_SIZE}")
    log.info(f"Waiting {STARTUP_DELAY}s for DB to be ready...")
    time.sleep(STARTUP_DELAY)
    wait_for_db(DB_CONN)

    total_inserted = 0
    total_updated  = 0
    loop_count     = 0

    # autocommit=False so we control transaction boundaries explicitly
    with psycopg.connect(DB_CONN, autocommit=False) as conn:
        while _RUNNING:
            loop_start = time.monotonic()

            # ── Insert new transactions ──────────────────────────────────
            new_rows = [generate_transaction() for _ in range(BATCH_SIZE)]
            try:
                inserted = insert_batch(conn, new_rows)
                total_inserted += inserted
            except Exception as e:
                log.error(f"Insert failed: {e}")
                conn.rollback()

            # ── Update existing transaction statuses ─────────────────────
            try:
                updated = update_statuses(conn, limit=BATCH_SIZE * 2)
                total_updated += updated
            except Exception as e:
                log.error(f"Update failed: {e}")
                conn.rollback()

            loop_count += 1

            # ── Log progress every 30 loops ──────────────────────────────
            if loop_count % 30 == 0:
                log.info(
                    f"Progress | inserted={total_inserted:,} "
                    f"updated={total_updated:,} "
                    f"loops={loop_count:,}"
                )

            # ── Rate limiting ────────────────────────────────────────────
            # Sleep to hit target INSERT_RATE tx/s
            elapsed = time.monotonic() - loop_start
            target_interval = BATCH_SIZE / INSERT_RATE
            sleep_time = max(0.0, target_interval - elapsed)
            time.sleep(sleep_time)

    log.info(
        f"Generator stopped | total_inserted={total_inserted:,} "
        f"total_updated={total_updated:,}"
    )


if __name__ == "__main__":
    main()