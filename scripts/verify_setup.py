"""
verify_setup.py
───────────────
Run this from your host machine AFTER `docker compose up` to confirm:
  1. Postgres source is reachable
  2. Transactions table exists and is being populated
  3. Watermark table is seeded
  4. Status updates are happening (updated_at changing)
  5. Generator throughput is within expected range

Usage:
    pip install psycopg[binary]
    python scripts/verify_setup.py
"""

import os
import sys
import time
from datetime import datetime, timezone

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    print("❌  psycopg not installed. Run: pip install 'psycopg[binary]'")
    sys.exit(1)

# ── Config ──────────────────────────────────────────────────────────────────
# Port 5433 because we mapped container's 5432 → host's 5433
DSN = os.environ.get(
    "SOURCE_DB_CONN",
    "postgresql://pipeline:pipeline@localhost:5433/transactions_db"
)

PASS = "✅"
FAIL = "❌"
WARN = "⚠️ "

def header(text: str):
    print(f"\n{'─' * 55}")
    print(f"  {text}")
    print(f"{'─' * 55}")

def check(label: str, passed: bool, detail: str = ""):
    icon = PASS if passed else FAIL
    suffix = f"  ({detail})" if detail else ""
    print(f"  {icon}  {label}{suffix}")
    return passed


def run_checks():
    all_passed = True

    header("1. Connectivity")
    try:
        conn = psycopg.connect(DSN, row_factory=dict_row, connect_timeout=5)
        check("Connected to postgres-source", True, "localhost:5433")
    except Exception as e:
        check("Connected to postgres-source", False, str(e))
        print(f"\n  {FAIL}  Cannot continue — DB unreachable.")
        print(f"       Is Docker running? Try: docker compose ps")
        sys.exit(1)


    header("2. Schema")

    # Check transactions table exists
    row = conn.execute("""
        SELECT COUNT(*) AS cnt
        FROM   information_schema.tables
        WHERE  table_name = 'transactions'
    """).fetchone()
    ok = row["cnt"] == 1
    all_passed &= check("transactions table exists", ok)

    # Check watermark table
    row = conn.execute("""
        SELECT COUNT(*) AS cnt
        FROM   information_schema.tables
        WHERE  table_name = 'pipeline_watermarks'
    """).fetchone()
    ok = row["cnt"] == 1
    all_passed &= check("pipeline_watermarks table exists", ok)

    # Check updated_at index
    row = conn.execute("""
        SELECT COUNT(*) AS cnt
        FROM   pg_indexes
        WHERE  tablename = 'transactions'
          AND  indexname  = 'idx_transactions_updated_at'
    """).fetchone()
    ok = row["cnt"] == 1
    all_passed &= check("updated_at index exists", ok)

    # Check trigger
    row = conn.execute("""
        SELECT COUNT(*) AS cnt
        FROM   information_schema.triggers
        WHERE  trigger_name = 'trg_transactions_updated_at'
    """).fetchone()
    ok = row["cnt"] == 1
    all_passed &= check("updated_at trigger exists", ok)


    header("3. Data volume")

    row = conn.execute("SELECT COUNT(*) AS total FROM transactions").fetchone()
    total = row["total"]
    ok = total > 0
    all_passed &= check("Transactions table has data", ok, f"{total:,} rows")

    # Status distribution
    rows = conn.execute("""
        SELECT status, COUNT(*) AS cnt
        FROM   transactions
        GROUP  BY status
        ORDER  BY cnt DESC
    """).fetchall()

    print(f"\n  Status breakdown:")
    for r in rows:
        bar = "█" * min(30, r["cnt"] // max(1, total // 30))
        print(f"    {r['status']:12s}  {r['cnt']:6,}  {bar}")


    header("4. Generator throughput (10 second sample)")

    t1_row = conn.execute("SELECT COUNT(*) AS cnt, MAX(created_at) AS newest FROM transactions").fetchone()
    t1_count  = t1_row["cnt"]
    t1_newest = t1_row["newest"]

    print(f"  Sampling for 10 seconds...")
    time.sleep(10)

    t2_row = conn.execute("SELECT COUNT(*) AS cnt, MAX(created_at) AS newest FROM transactions").fetchone()
    t2_count  = t2_row["cnt"]
    delta     = t2_count - t1_count
    rate      = delta / 10.0

    ok = rate >= 1.0
    all_passed &= check(
        "Generator is inserting rows",
        ok,
        f"{delta} new rows in 10s ≈ {rate:.1f} rows/sec"
    )
    if not ok:
        print(f"  {WARN}  Is the generator container running? docker compose ps")


    header("5. Watermark table")

    wm = conn.execute("""
        SELECT pipeline_name, last_updated_at, rows_processed
        FROM   pipeline_watermarks
        WHERE  pipeline_name = 'hourly_batch_pipeline'
    """).fetchone()

    if wm:
        check("Watermark record seeded", True, f"last_updated_at = {wm['last_updated_at']}")
        check("Initial watermark at epoch", wm["last_updated_at"].year < 1972,
              "(correct — pipeline hasn't run yet)")
    else:
        all_passed &= check("Watermark record seeded", False, "no row found")


    header("6. updated_at trigger test")

    # Grab one pending tx, manually update it, verify updated_at changed
    tx = conn.execute("""
        SELECT id, status, updated_at
        FROM   transactions
        WHERE  status = 'pending'
        LIMIT  1
    """).fetchone()

    if tx:
        old_updated_at = tx["updated_at"]
        time.sleep(0.1)  # small sleep so timestamps differ
        conn.execute("""
            UPDATE transactions SET status = 'processing' WHERE id = %s
        """, (tx["id"],))
        conn.commit()

        new_row = conn.execute(
            "SELECT updated_at FROM transactions WHERE id = %s",
            (tx["id"],)
        ).fetchone()

        trigger_fired = new_row["updated_at"] > old_updated_at
        all_passed &= check(
            "updated_at trigger fires on UPDATE",
            trigger_fired,
            f"{old_updated_at.isoformat()} → {new_row['updated_at'].isoformat()}"
        )
    else:
        print(f"  {WARN}  No pending transactions found — skipping trigger test")


    header("Summary")
    if all_passed:
        print(f"  {PASS}  All checks passed. Ready for Phase 3 (Airflow DAG).")
    else:
        print(f"  {FAIL}  Some checks failed. Fix issues above before proceeding.")

    conn.close()
    return all_passed


if __name__ == "__main__":
    success = run_checks()
    print()
    sys.exit(0 if success else 1)