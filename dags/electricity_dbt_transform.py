#!/usr/bin/env python3
"""
dbt Transformation DAG (Professor's Pattern)
Runs dbt models to transform raw data into analytics-ready tables
- Components: deps → Transform → Test → Snapshot → Document
- Backfill support with catchup
- Idempotent: dbt models use incremental strategies
- Error handling: BashOperator fails on non-zero exit codes
- Transaction safety: dbt handles transactions internally
Schedule: Run manually or after ETL completes
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Get Snowflake connection details with error handling
try:
    conn = BaseHook.get_connection('snowflake_conn')
except Exception as e:
    logger.error(f"Failed to get Snowflake connection: {e}")
    raise

# ========================
# DAG Configuration
# ========================
default_args = {
    'owner': 'data_eng',
    'start_date': datetime.now() - timedelta(days=1),  # Yesterday - prevents backfill
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=20),
    'env': {
        'DBT_USER': conn.login,
        'DBT_PASSWORD': conn.password,
        'DBT_ACCOUNT': conn.extra_dejson.get('account', ''),
        'DBT_SCHEMA': 'ELECTRICITY_ANALYTICS',  # Force analytics schema for dbt models
        'DBT_DATABASE': conn.extra_dejson.get('database', 'USER_DB_HIPPO'),
        'DBT_ROLE': conn.extra_dejson.get('role', 'TRAINING_ROLE'),
        'DBT_WAREHOUSE': conn.extra_dejson.get('warehouse', 'HIPPO_QUERY_WH'),
        'DBT_TYPE': 'snowflake'
    }
}

dag = DAG(
    'electricity_dbt_transform',
    default_args=default_args,
    description='dbt: deps → Transform → Test → Snapshot → Document',
    schedule_interval='30 * * * *',  # Run 30 minutes after ETL (hourly at :30)
    catchup=False,  # Disable backfill - only run when triggered
    max_active_runs=1,  # Process one at a time
    tags=['dbt', 'Transform', 'Hourly', 'Production'],
)

DBT_PROJECT_DIR = '/opt/airflow/dbt/electricity_forecast'
DBT_PROFILES_DIR = '/opt/airflow/dbt'
DBT_BIN = '/home/airflow/.local/bin/dbt'  # Full path to dbt executable

# ========================
# dbt Components (following professor's workflow)
# Note: BashOperator automatically handles errors (non-zero exit = task failure)
# dbt handles transactions internally (BEGIN/COMMIT/ROLLBACK)
# dbt models are idempotent by design (incremental strategies)
# ========================

# Environment variables for dbt (from Snowflake connection)
dbt_env = {
    'DBT_USER': conn.login,
    'DBT_PASSWORD': conn.password,
    'DBT_ACCOUNT': conn.extra_dejson.get('account', 'sfedu02-lvb17920'),
    'DBT_SCHEMA': 'ELECTRICITY_ANALYTICS',  # Force analytics schema for dbt models
    'DBT_DATABASE': conn.extra_dejson.get('database', 'USER_DB_HIPPO'),
    'DBT_ROLE': conn.extra_dejson.get('role', 'TRAINING_ROLE'),
    'DBT_WAREHOUSE': conn.extra_dejson.get('warehouse', 'HIPPO_QUERY_WH'),
    'DBT_TYPE': 'snowflake'
}

# 0. Install dependencies (idempotent)
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command=f'{DBT_BIN} deps --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}',
    env=dbt_env,  # Pass environment variables
    retries=2,
    retry_delay=timedelta(minutes=2),
    dag=dag,
)

# 1. TRANSFORM - Run models (staging → marts)
# dbt handles transactions internally with BEGIN/COMMIT/ROLLBACK
# Models are idempotent (can re-run safely)
dbt_transform = BashOperator(
    task_id='dbt_transform',
    bash_command=f'{DBT_BIN} run --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}',
    env=dbt_env,  # Pass environment variables
    retries=2,
    retry_delay=timedelta(minutes=2),
    dag=dag,
)

# 2. TEST - Data quality validation
# Tests fail the task if data quality issues found
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=f'{DBT_BIN} test --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}',
    env=dbt_env,  # Pass environment variables
    retries=1,
    retry_delay=timedelta(minutes=1),
    dag=dag,
)

# 3. SNAPSHOT - Version control for dimension tables (SCD Type 2)
# Idempotent: Only captures changes since last snapshot
dbt_snapshot = BashOperator(
    task_id='dbt_snapshot',
    bash_command=f'{DBT_BIN} snapshot --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}',
    env=dbt_env,  # Pass environment variables
    retries=2,
    retry_delay=timedelta(minutes=2),
    dag=dag,
)

# 4. DOCUMENT - Generate documentation (idempotent)
dbt_document = BashOperator(
    task_id='dbt_document',
    bash_command=f'{DBT_BIN} docs generate --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}',
    env=dbt_env,  # Pass environment variables
    retries=1,
    retry_delay=timedelta(minutes=1),
    dag=dag,
)

# ========================
# Task Dependencies (Professor's proven workflow)
# deps → transform → test → snapshot → docs
# 
# Error Handling:
# - BashOperator fails task on non-zero exit code
# - Airflow retries based on retry settings
# - dbt handles SQL transactions internally (BEGIN/COMMIT/ROLLBACK)
# 
# Idempotency:
# - dbt models use CREATE OR REPLACE / incremental strategies
# - Snapshots only capture new changes
# - Can safely re-run any task
# 
# Backfill Support:
# - catchup=True allows historical runs
# - dbt models process all available data
# ========================

dbt_deps >> dbt_transform >> dbt_test >> dbt_snapshot >> dbt_document
