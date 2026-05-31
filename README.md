# nano-chop: Python CDC Pipeline for Snowflake

A lightweight, high-performance Python implementation of Snowflake CDC (Change Data Capture) processing.  
Replaces JavaScript stored procedures with pure Python orchestration running in Airflow.

## What It Does

- **DDL Syncer**: Detects schema changes daily, updates metadata, auto-alters IMPORT tables
- **Streams Processor**: Processes CDC records every 10 minutes, applies INSERT/UPDATE/soft-DELETE to IMPORT schema

Data flow: `LANDING (JSON/VARIANT)` → nano-chop (Python) → `IMPORT (typed columns)` → `admin.snowflake_events` (logging)

---

## Local Development Setup

### Prerequisites

- [Colima](https://github.com/abiosoft/colima) with Docker running
- Snowflake account with key-pair authentication

### Colima Setup (Apple Silicon Macs)

If you're on an Apple Silicon Mac running colima with `arch: x86_64`, you must set `cpuType: max` in `~/.colima/default/colima.yaml` — otherwise compiled packages in the Airflow image will crash with `Illegal instruction`.

```yaml
# ~/.colima/default/colima.yaml
cpuType: "max"
```

Apply the change by restarting colima:

```bash
colima stop && colima start
```

### Quick Start

```bash
# 1. Copy and configure credentials
cp .env.example .env
# Edit .env with your Snowflake credentials

# 2. Start all services (Postgres + Airflow webserver + scheduler)
docker-compose up -d

# 3. Open the UI
open http://localhost:8080
# Username: airflow  |  Password: airflow
```

The first startup runs `airflow db migrate` and creates the admin user automatically via an init container.

### Services

| Service | Port | Description |
|---------|------|-------------|
| Webserver | 8080 | Airflow UI |
| Scheduler | — | DAG scheduling |
| Postgres | 5432 | Airflow metadata DB |

### Useful Commands

```bash
docker-compose ps                    # Status of all containers
docker-compose logs -f webserver     # Tail webserver logs
docker-compose logs -f scheduler     # Tail scheduler logs
docker-compose down                  # Stop all services
docker-compose down -v               # Stop and delete Postgres data
```

---

## Installation

### Prerequisites

- Python 3.9+
- Snowflake account with key-pair authentication
- Private key file (PEM format, unencrypted or passphrase-protected)

### 1. Generate Snowflake Key Pair (if needed)

```bash
openssl genrsa -out rsa_key.p8 2048

# Add the public key to your Snowflake user
snowsql -c <connection> -f - <<EOF
ALTER USER SVC_NANO_CHOP SET RSA_PUBLIC_KEY = '<contents-of-rsa_key.pub>';
EOF
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Credentials

```bash
cp .env.example .env
# Fill in SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH, etc.
```

### 4. Create Admin Tables in Snowflake (one-time)

```sql
CREATE SCHEMA IF NOT EXISTS ADMIN;

CREATE TABLE IF NOT EXISTS ADMIN.DDL_CONTROL (
    TABLE_NAME VARCHAR,
    PRIMARY_KEYS VARCHAR,
    PRIMARY_KEYS_STR VARCHAR,
    PRIMARY_KEYS_PART VARCHAR,
    SELECT_STR VARCHAR,
    INSERT_STR VARCHAR,
    UPDATE_STR VARCHAR,
    DELETE_STR VARCHAR,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR DEFAULT CURRENT_USER(),
    UPDATED_AT TIMESTAMP_NTZ,
    UPDATED_BY VARCHAR,
    PRIMARY KEY (TABLE_NAME)
);

CREATE TABLE IF NOT EXISTS ADMIN.SNOWFLAKE_EVENTS (
    TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RECORD_TYPE VARCHAR,
    VALUE VARCHAR,
    RECORD_ATTRIBUTES VARIANT
);
```

### 5. Configure Tables

Edit `config/tables.yaml`:

```yaml
environments:
  prod:
    account: "abc123.snowflakecomputing.com"
    user: "SVC_NANO_CHOP"
    warehouse: "COMPUTE_WH"
    database: "YOUR_DB"
    role: "SYSADMIN"
    private_key_path: "${SNOWFLAKE_PRIVATE_KEY_PATH}"
    private_key_passphrase: ""
    landing_schema: "LANDING"
    target_schema: "IMPORT"
    tmp_schema: "TMP"

tables:
  agreements:
    - agreement
    - member_agreement
  benefits:
    - benefit_type

ddl_syncer:
  default_run_mode: ""  # "" = last 5 days | "FULL" | "TEST"

streams_processor:
  default_bypass_landing_key: -1
  max_workers: 8
```

---

## Usage

### Via Airflow UI

1. Open http://localhost:8080, log in with `airflow` / `airflow`
2. Go to **DAGs** → select `nano_chop` → click **Trigger DAG**

### Via Python

```python
from nano_chop.ddl_syncer import sync_all_tables
from nano_chop.streams_processor import process_all_tables
import yaml

with open('config/tables.yaml') as f:
    config = yaml.safe_load(f)

# Sync DDL metadata
sync_all_tables(config, env='prod', run_mode='')

# Process CDC streams
process_all_tables(config, env='prod')
```

---

## Configuration

### Run Modes

| Mode | Behaviour |
|------|-----------|
| `""` (default) | Process LANDING data from last 5 days |
| `"FULL"` | Process all data in LANDING table |
| `"TEST"` | Process only the latest batch/day |

### Adding a New Table

1. Add it under a group in `config/tables.yaml`:
   ```yaml
   tables:
     my_group:
       - new_table
   ```

2. Ensure Snowflake has `LANDING.NEW_TABLE` with `RECORD_METADATA:key` and `RECORD_CONTENT` columns.

3. Run DDL Syncer first — it populates `ADMIN.DDL_CONTROL` and auto-creates the `IMPORT` table.

---

## Monitoring

```sql
-- Recent runs
SELECT TIMESTAMP, RECORD_TYPE, VALUE, RECORD_ATTRIBUTES
FROM ADMIN.SNOWFLAKE_EVENTS
WHERE TIMESTAMP >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY TIMESTAMP DESC LIMIT 100;

-- Per-table stats
SELECT
    RECORD_ATTRIBUTES:table_name::VARCHAR as table_name,
    RECORD_ATTRIBUTES:record_processed::INT as records,
    RECORD_ATTRIBUTES:insert_count::INT as inserts,
    RECORD_ATTRIBUTES:update_count::INT as updates,
    RECORD_ATTRIBUTES:delete_count::INT as deletes,
    TIMESTAMP
FROM ADMIN.SNOWFLAKE_EVENTS
WHERE RECORD_ATTRIBUTES:table_name IS NOT NULL
ORDER BY TIMESTAMP DESC LIMIT 50;
```

---

## Module Reference

### `nano_chop.connection`
- `get_connection(config: dict)` → `SnowflakeConnection`
- `ConnectionPool(config: dict, pool_size: int)` — thread-safe connection pool

### `nano_chop.sql_builder`
- `build_pk_join_condition(keys)` → SQL join condition
- `build_select_str(columns, primary_keys)` → SELECT clause with type casting
- `build_update_str(columns)` → UPDATE clause
- `build_insert_str(columns)` → INSERT clause
- `apply_debezium_mapping(...)` → Handle `__debezium_unavailable_value`

### `nano_chop.ddl_syncer`
- `sync_table(conn, table_name, target_schema, run_mode='')` → list of result strings
- `sync_all_tables(config, env, groups=None, run_mode='', max_workers=4)` → dict of results

### `nano_chop.streams_processor`
- `process_table(conn, table_name, schema, bypass_landing_key=-1)` → list of result strings
- `process_all_tables(config, env, groups=None, bypass_landing_key=-1, max_workers=8)` → dict of results

### `nano_chop.event_logger`
- `log_event(conn, level, status, attributes)` → logs to `ADMIN.SNOWFLAKE_EVENTS`

---

## Testing

```bash
python -m pytest tests/ -v
```

| Test file | What it covers |
|-----------|----------------|
| `test_sql_builder.py` | SQL string generation (no DB needed) |
| `test_ddl_syncer.py` | DDL logic with mocked Snowflake |
| `test_streams_processor.py` | CDC processing with mocked cursors |

---

## Performance

- 8 tables processed in parallel (configurable via `max_workers`)
- ~5 Snowflake round-trips per table (BEGIN → TEMP → UPDATE → INSERT → DELETE/COMMIT)
- ~30–60 seconds for 20+ tables with 1M+ records each
- Incremental by `landing_key` — skips tables with no new data in <10ms

---

## Troubleshooting

**`Illegal instruction (core dumped)` in Airflow containers**  
Set `cpuType: max` in `~/.colima/default/colima.yaml` and restart colima. See [Colima Setup](#colima-setup-apple-silicon-macs) above.

**Webserver shows "No response from gunicorn master within 120 seconds"**  
Same root cause — CPU type in colima. Apply the fix above.

**`ModuleNotFoundError: No module named 'nano_chop'`**  
The `src/` directory must be mounted and `PYTHONPATH=/opt/airflow/src` must be set. Both are handled in `docker-compose.yml`.

**`FileNotFoundError: config/tables.yaml`**  
Copy and configure `config/tables.yaml`. The `config/` directory is mounted into the container.

**"Private key not found"**  
Check `SNOWFLAKE_PRIVATE_KEY_PATH` in `.env` — it must be an absolute path.

**"Table XYZ not in ADMIN.DDL_CONTROL"**  
Run the DDL Syncer DAG first before triggering the Streams Processor.

---

## License

Proprietary. See your organization's policies.
