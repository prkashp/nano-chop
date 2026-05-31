# nano-chop Airflow DAGs

Three production-ready DAGs for orchestrating the nano-chop CDC pipeline.

## DAGs Overview

### 1. `nano_chop_ddl_syncer` (Daily)

**Schedule**: `0 0 * * *` (Daily at 00:00 UTC)

**Purpose**: Detect schema changes in LANDING tables, update `admin.ddl_control`, and alter IMPORT tables with new columns.

**Tasks**:
- `validate_config` — Check YAML config is valid
- `run_ddl_syncer` — Execute DDL sync for all tables

**XCom Output**: `ddl_sync_results` → dict of {table_name: [messages]}

**Failure Handling**: Retries once after 5 minutes, sends email alert on final failure

---

### 2. `nano_chop_streams_processor` (Every 10 Minutes)

**Schedule**: `*/10 * * * *` (Every 10 minutes)

**Purpose**: Read new CDC records from LANDING, apply INSERT/UPDATE/soft-DELETE to IMPORT tables, and log run statistics.

**Tasks**:
- `validate_config` — Check YAML config is valid
- `run_streams_processor` — Execute streams processor for all tables

**XCom Output**: `streams_results` → dict of {table_name: [messages]}

**Failure Handling**: Retries once after 5 minutes, sends email alert on final failure

---

### 3. `nano_chop_full_pipeline` (Daily at 01:00 UTC)

**Schedule**: `0 1 * * *` (Daily at 01:00 UTC, after DDL sync completes)

**Purpose**: One combined DAG that runs both DDL sync and streams processor in sequence (useful for full daily refresh).

**Tasks**:
- `validate_config` — Check YAML config is valid
- `ddl_syncer` → `streams_processor` (sequential)

---

## Setup

### 1. Add to Airflow DAG Directory

Copy `dags/nano_chop_dag.py` to your Airflow DAG folder:

```bash
cp dags/nano_chop_dag.py /path/to/airflow/dags/
```

### 2. Configure Airflow Variables (Optional)

Set Airflow Variables to customize behavior per environment:

```python
# Via Airflow UI Admin → Variables

# DDL Syncer variables
NANO_CHOP_ENV = "prod"                          # Default: prod
NANO_CHOP_DDL_GROUPS = "agreements,benefits"    # Specific groups (or empty for all)
NANO_CHOP_DDL_RUN_MODE = ""                     # "" (5 days), "FULL", or "TEST"

# Streams Processor variables
NANO_CHOP_STREAMS_GROUPS = "agreements,benefits" # Specific groups (or empty for all)
NANO_CHOP_STREAMS_BYPASS_KEY = "-1"              # -1 = use last logged key

# Alerting
NANO_CHOP_ALERT_EMAIL = "data-ops@example.com,user@example.com"
```

Or via CLI:

```bash
airflow variables set NANO_CHOP_ENV "prod"
airflow variables set NANO_CHOP_ALERT_EMAIL "data-ops@example.com"
```

### 3. Ensure Dependencies are Installed

In your Airflow environment:

```bash
pip install -r /path/to/nano-chop/requirements.txt
```

### 4. Set PYTHONPATH (if needed)

If Airflow doesn't automatically discover the `nano_chop` module:

```bash
# In airflow.cfg or env:
export PYTHONPATH="${PYTHONPATH}:/path/to/nano-chop/src"
```

---

## Usage

### Trigger via Airflow UI

1. Navigate to the DAG
2. Click "Trigger DAG" button
3. (Optional) Override variables in "Conf" field (JSON format)

### Trigger via CLI

```bash
# Trigger DDL Syncer
airflow dags trigger nano_chop_ddl_syncer

# Trigger Streams Processor
airflow dags trigger nano_chop_streams_processor

# Trigger Full Pipeline
airflow dags trigger nano_chop_full_pipeline

# Trigger with config override
airflow dags trigger nano_chop_streams_processor \
  -c '{"NANO_CHOP_STREAMS_BYPASS_KEY": "1000"}'
```

### View Logs

```bash
# Via CLI
airflow logs nano_chop_ddl_syncer run_ddl_syncer 2026-05-20T00:00:00+00:00

# Or in Airflow UI: Admin → Logs
```

---

## Monitoring

### Check DAG Status

```sql
-- Check latest run for each table
SELECT 
    RECORD_ATTRIBUTES:table_name::VARCHAR as table_name,
    RECORD_ATTRIBUTES:operation::VARCHAR as operation,
    VALUE,
    TIMESTAMP,
    RECORD_ATTRIBUTES:record_processed::INT as records
FROM ADMIN.SNOWFLAKE_EVENTS
WHERE TIMESTAMP >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY TIMESTAMP DESC
LIMIT 100;
```

### Check for Failures

```sql
-- Recent failures
SELECT * FROM ADMIN.SNOWFLAKE_EVENTS
WHERE VALUE = 'FAILED'
  AND TIMESTAMP >= DATEADD(day, -1, CURRENT_TIMESTAMP())
ORDER BY TIMESTAMP DESC;
```

### Check DAG Runs in Airflow

```bash
airflow dags list-runs --dag-id nano_chop_ddl_syncer
airflow dags list-runs --dag-id nano_chop_streams_processor
```

---

## Configuration

### DDL Run Modes

The DDL syncer supports three run modes (set via `NANO_CHOP_DDL_RUN_MODE` variable):

- **`""`** (empty, default): Process LANDING data from last 5 days (recommended)
- **`"FULL"`**: Process all data in LANDING table (slow, not recommended for large tables)
- **`"TEST"`**: Process only the latest batch/day (fast, for testing)

### Selective Table Groups

Run DDL sync or streams processor on specific table groups only:

```bash
# DDL sync only "agreements" and "benefits" groups
airflow dags trigger nano_chop_ddl_syncer \
  -c '{"NANO_CHOP_DDL_GROUPS": "agreements,benefits"}'

# Streams processor for same groups
airflow dags trigger nano_chop_streams_processor \
  -c '{"NANO_CHOP_STREAMS_GROUPS": "agreements,benefits"}'
```

### Bypass Landing Key (For Reprocessing)

To reprocess records from a specific landing_key (useful for fixing data):

```bash
# Reprocess from landing_key=5000
airflow dags trigger nano_chop_streams_processor \
  -c '{"NANO_CHOP_STREAMS_BYPASS_KEY": "5000"}'

# Normal (use last logged key)
airflow dags trigger nano_chop_streams_processor \
  -c '{"NANO_CHOP_STREAMS_BYPASS_KEY": "-1"}'
```

---

## Troubleshooting

### Error: "Environment 'prod' not found in config"

**Cause**: Environment not defined in `config/tables.yaml`

**Fix**: Add environment to YAML:

```yaml
environments:
  prod:
    account: "..."
    user: "..."
    # ... etc
```

### Error: "Module 'nano_chop' not found"

**Cause**: Airflow can't find nano_chop package

**Fix**: Add to Airflow's PYTHONPATH:

```bash
export PYTHONPATH="${PYTHONPATH}:/path/to/nano-chop/src"

# Or in airflow.cfg:
[core]
pythonpath = /path/to/nano-chop/src
```

### Error: "Private key not found"

**Cause**: Snowflake private key path not found

**Fix**: Verify path in `config/tables.yaml`:

```yaml
private_key_path: "/absolute/path/to/rsa_key.p8"
```

### Streams Processor Running Too Long

**Cause**: Processing many tables or large batches sequentially

**Fix**: Increase `max_workers` in `config/tables.yaml`:

```yaml
streams_processor:
  max_workers: 16  # Increase parallel tables
```

---

## Advanced: Custom Task Configuration

You can extend the DAGs with additional tasks. Example: add a Slack notification:

```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

notify_success = SlackWebhookOperator(
    task_id="notify_slack",
    http_conn_id="slack_webhook",
    message="✓ nano-chop streams processor completed successfully"
)

run_streams_processor >> notify_success
```

---

## Performance

**Typical run times** (varies by table count and data volume):

- **DDL Syncer**: 30-60 seconds for 20+ tables
- **Streams Processor**: 1-5 minutes per run (incremental, skips tables with no new data)
- **Full Pipeline**: 2-10 minutes total

**Key factors**:
- Number of tables in config
- Record volume in LANDING tables since last run
- Snowflake warehouse compute power
- Network latency

---

## Support

For issues:

1. Check Airflow task logs (Admin → Logs)
2. Check Snowflake event logs:
   ```sql
   SELECT * FROM ADMIN.SNOWFLAKE_EVENTS WHERE TIMESTAMP >= DATEADD(day, -1, CURRENT_TIMESTAMP());
   ```
3. Review nano-chop logs in Snowflake query history
