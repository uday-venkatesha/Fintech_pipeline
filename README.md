# Fintech Batch Pipeline — Local Dev

## Project structure

```
fintech_pipeline/
├── docker-compose.yml          # Full stack definition
├── airflow/
│   ├── dags/                   # Airflow DAGs (Phase 3+)
│   ├── logs/                   # Airflow task logs
│   └── plugins/
├── generator/
│   └── generate_transactions.py  # Continuous data generator
├── scripts/
│   ├── init_source_db.sql      # Source DB schema
│   └── verify_setup.py         # Phase 2 health check
├── warehouse/                  # DuckDB files (Phase 3+)
└── tests/                      # Data quality tests (Phase 5+)
```

---

## Phase 2 — Bring up the stack

### Prerequisites
- Docker Desktop running (4 GB RAM minimum allocated)
- Python 3.10+ on your host (for verify script only)

### Step 1 — Set file permissions (Mac/Linux only)
```bash
mkdir -p airflow/logs airflow/dags airflow/plugins warehouse
chmod -R 777 airflow/logs
```

### Step 2 — Initialise Airflow
This runs once to create the DB schema and admin user.
```bash
docker compose up airflow-init
```
Wait for: `Airflow initialised ✓` then it exits on its own.

### Step 3 — Start everything
```bash
docker compose up -d
```

### Step 4 — Check all containers are healthy
```bash
docker compose ps
```
Expected output — all services should show `running` or `healthy`:
```
NAME                    STATUS
airflow-scheduler       running
airflow-webserver       running (healthy)
airflow-worker          running
airflow-triggerer       running
postgres-source         running (healthy)
postgres-meta           running (healthy)
redis                   running (healthy)
tx-generator            running
```

> The generator installs its dependencies on first start.
> Give it ~30 seconds before checking logs.

### Step 5 — Watch the generator
```bash
docker compose logs -f generator
```
You should see:
```
[GENERATOR] INFO Connected to source DB ✓
[GENERATOR] INFO Progress | inserted=300 updated=180 loops=30
[GENERATOR] INFO Progress | inserted=600 updated=350 loops=60
```

### Step 6 — Verify everything
Install the psycopg driver on your host:
```bash
pip install "psycopg[binary]"
```
Run the verification script:
```bash
python scripts/verify_setup.py
```
All 6 checks should pass with ✅.

### Step 7 — Open Airflow UI
http://localhost:8080
- Username: `admin`
- Password: `admin`

No DAGs yet — those come in Phase 3.

---

## Useful commands

```bash
# Connect directly to source DB (from host)
psql postgresql://pipeline:pipeline@localhost:5433/transactions_db

# Watch live row counts
watch -n2 'psql postgresql://pipeline:pipeline@localhost:5433/transactions_db \
  -c "SELECT status, COUNT(*) FROM transactions GROUP BY status"'

# Tail all logs
docker compose logs -f

# Stop everything (data persists in volumes)
docker compose down

# Full reset including data volumes
docker compose down -v
```

---

## What happens if the generator crashes?

The `restart: unless-stopped` policy on the generator service means
Docker will automatically restart it. This simulates production resilience.

Try it:
```bash
docker compose restart generator
docker compose logs -f generator
```

You will see it reconnect and resume inserting without any data loss.
This is intentional — Phase 4 will handle the pipeline-side failure scenarios.