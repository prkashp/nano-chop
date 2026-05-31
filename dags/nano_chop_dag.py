"""
Airflow DAG for nano-chop CDC Pipeline.

Schedules:
- DDL Syncer: Daily at 00:00 UTC
- Streams Processor: Every 10 minutes

This DAG orchestrates:
1. DDL schema detection and IMPORT table alteration
2. CDC record loading from LANDING to IMPORT
3. Event logging to admin.snowflake_events
4. Failure alerting (optional)
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
import yaml

# Add nano_chop to path
REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from nano_chop.ddl_syncer import sync_all_tables
from nano_chop.streams_processor import process_all_tables


# ============================================================================
# Configuration
# ============================================================================

# Load config from YAML
CONFIG_PATH = REPO_ROOT / "config" / "tables.yaml"
with open(CONFIG_PATH) as f:
    CONFIG = yaml.safe_load(f)

# Get environment from Airflow variable or default to 'prod'
ENVIRONMENT = Variable.get("NANO_CHOP_ENV", "prod")

# Alerting email (can override in Airflow)
ALERT_EMAIL = Variable.get("NANO_CHOP_ALERT_EMAIL", default_var=None)
if ALERT_EMAIL is None:
    ALERT_EMAIL = ["data-ops@example.com"]
elif isinstance(ALERT_EMAIL, str):
    ALERT_EMAIL = [e.strip() for e in ALERT_EMAIL.split(",")]

# Default args for both DAGs
DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 1),
    "email": ALERT_EMAIL,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================================
# Helper Functions
# ============================================================================

def _run_ddl_sync(**context):
    """
    Run DDL syncer for all or specific table groups.

    Airflow variables:
    - NANO_CHOP_DDL_GROUPS: comma-separated list of groups (e.g., "agreements,benefits")
      If not set, syncs all tables.
    - NANO_CHOP_DDL_RUN_MODE: "", "FULL", or "TEST" (default: "")
    """
    groups = Variable.get("NANO_CHOP_DDL_GROUPS", default_var=None)
    if groups:
        groups = [g.strip() for g in groups.split(",")]

    run_mode = Variable.get("NANO_CHOP_DDL_RUN_MODE", default_var="")

    try:
        results = sync_all_tables(
            config=CONFIG,
            env=ENVIRONMENT,
            groups=groups,
            run_mode=run_mode,
            max_workers=4
        )

        # Log results
        for table, messages in results.items():
            context["task_instance"].log.info(f"{table}: {messages}")

        # Check for errors
        errors = {
            table: msgs
            for table, msgs in results.items()
            if any("ERROR" in msg for msg in msgs)
        }

        if errors:
            raise AirflowException(f"DDL sync errors: {errors}")

        context["task_instance"].xcom_push(
            key="ddl_sync_results",
            value=results
        )

        return results

    except Exception as e:
        context["task_instance"].log.error(f"DDL sync failed: {str(e)}")
        raise


def _run_streams_processor(**context):
    """
    Run streams processor for all or specific table groups.

    Airflow variables:
    - NANO_CHOP_STREAMS_GROUPS: comma-separated list of groups (e.g., "agreements,benefits")
      If not set, processes all tables.
    - NANO_CHOP_STREAMS_BYPASS_KEY: Override last landing key (-1 uses last logged)
    """
    groups = Variable.get("NANO_CHOP_STREAMS_GROUPS", default_var=None)
    if groups:
        groups = [g.strip() for g in groups.split(",")]

    bypass_key = Variable.get("NANO_CHOP_STREAMS_BYPASS_KEY", default_var="-1")
    bypass_key = float(bypass_key)

    try:
        results = process_all_tables(
            config=CONFIG,
            env=ENVIRONMENT,
            groups=groups,
            bypass_landing_key=bypass_key
        )

        # Log results
        for table, messages in results.items():
            context["task_instance"].log.info(f"{table}: {messages}")

        # Check for errors
        errors = {
            table: msgs
            for table, msgs in results.items()
            if any("ERROR" in msg for msg in msgs)
        }

        if errors:
            raise AirflowException(f"Streams processor errors: {errors}")

        context["task_instance"].xcom_push(
            key="streams_results",
            value=results
        )

        return results

    except Exception as e:
        context["task_instance"].log.error(f"Streams processor failed: {str(e)}")
        raise


def _validate_config(**context):
    """Validate that config is loaded and environment exists."""
    if ENVIRONMENT not in CONFIG.get("environments", {}):
        raise AirflowException(
            f"Environment '{ENVIRONMENT}' not found in config. "
            f"Available: {list(CONFIG.get('environments', {}).keys())}"
        )

    tables_config = CONFIG.get("tables", {})
    if not tables_config:
        raise AirflowException("No tables configured in config/tables.yaml")

    total_tables = sum(len(v) if isinstance(v, list) else 1 for v in tables_config.values())
    context["task_instance"].log.info(
        f"Loaded config for environment '{ENVIRONMENT}' with {total_tables} tables"
    )


def _success_callback(context):
    """Called on task success."""
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id

    # Log to file or send metric
    print(f"✓ {dag_id}.{task_id} succeeded at {datetime.now(timezone.utc).isoformat()}")


def _failure_callback(context):
    """Called on task failure."""
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    exception = context.get("exception")

    # Log to file or send metric
    print(f"✗ {dag_id}.{task_id} failed: {exception}")


# ============================================================================
# DDL Syncer DAG (Daily)
# ============================================================================

with DAG(
    dag_id="nano_chop_ddl_syncer",
    description="Daily DDL schema detection and IMPORT table alteration",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 0 * * *",  # Daily at 00:00 UTC
    catchup=False,
    tags=["nano-chop", "ddl-sync", "snowflake"],
) as ddl_dag:

    validate_config_ddl = PythonOperator(
        task_id="validate_config",
        python_callable=_validate_config,
        on_success_callback=_success_callback,
        on_failure_callback=_failure_callback,
    )

    run_ddl_sync = PythonOperator(
        task_id="run_ddl_syncer",
        python_callable=_run_ddl_sync,
        provide_context=True,
        on_success_callback=_success_callback,
        on_failure_callback=_failure_callback,
    )

    # Task dependency
    validate_config_ddl >> run_ddl_sync


# ============================================================================
# Streams Processor DAG (Every 10 minutes)
# ============================================================================

with DAG(
    dag_id="nano_chop_streams_processor",
    description="Every 10 mins: CDC record loading from LANDING to IMPORT",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/10 * * * *",  # Every 10 minutes
    catchup=False,
    tags=["nano-chop", "streams-processor", "cdc", "snowflake"],
) as streams_dag:

    validate_config_streams = PythonOperator(
        task_id="validate_config",
        python_callable=_validate_config,
        on_success_callback=_success_callback,
        on_failure_callback=_failure_callback,
    )

    run_streams_processor = PythonOperator(
        task_id="run_streams_processor",
        python_callable=_run_streams_processor,
        provide_context=True,
        on_success_callback=_success_callback,
        on_failure_callback=_failure_callback,
    )

    # Task dependency
    validate_config_streams >> run_streams_processor


# ============================================================================
# Combined DAG (Optional: Run both in sequence once per day)
# ============================================================================

with DAG(
    dag_id="nano_chop_full_pipeline",
    description="Combined: DDL sync → Streams processor (daily at 01:00 UTC)",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 1 * * *",  # Daily at 01:00 UTC (after DDL sync)
    catchup=False,
    tags=["nano-chop", "full-pipeline", "snowflake"],
) as full_dag:

    validate_config_full = PythonOperator(
        task_id="validate_config",
        python_callable=_validate_config,
        on_success_callback=_success_callback,
        on_failure_callback=_failure_callback,
    )

    ddl_sync_full = PythonOperator(
        task_id="ddl_syncer",
        python_callable=_run_ddl_sync,
        provide_context=True,
        on_success_callback=_success_callback,
        on_failure_callback=_failure_callback,
    )

    streams_full = PythonOperator(
        task_id="streams_processor",
        python_callable=_run_streams_processor,
        provide_context=True,
        on_success_callback=_success_callback,
        on_failure_callback=_failure_callback,
    )

    # Task dependencies: validate → DDL → streams
    validate_config_full >> ddl_sync_full >> streams_full
