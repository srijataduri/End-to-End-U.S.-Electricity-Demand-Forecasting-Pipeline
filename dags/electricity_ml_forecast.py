#!/usr/bin/env python3
"""
ML Forecasting DAG - Snowflake ML FORECAST (Professor's Pattern)
- Uses @task decorators
- BEGIN/COMMIT/ROLLBACK transactions everywhere
- Try/except/raise error handling
- Incremental load with backfill support
- Idempotent operations (CREATE OR REPLACE)
- Optimized for production
"""
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def return_snowflake_conn():
    """Get Snowflake cursor from Airflow connection"""
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        return conn.cursor()
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise


@task
def train(train_input_table, train_view, forecast_function_name, run_date, **context):
    """
    Train Snowflake ML FORECAST model
    - Create view with training data (last 90 days from run_date)
    - Create ML model with automatic feature engineering
    - Show evaluation metrics
    - Idempotent: CREATE OR REPLACE
    - Backfill support: Uses run_date for historical training
    """
    
    # Get Snowflake cursor (inside task, not at parse time)
    cur = return_snowflake_conn()
    
    try:
        # BEGIN transaction
        logger.info(f"Starting model training for run_date: {run_date}")
        cur.execute("BEGIN")
        
        # Create view with training data (90 days before run_date)
        # This allows backfill to work correctly
        create_view_sql = f"""
        CREATE OR REPLACE VIEW {train_view} AS
        SELECT 
            DATE_TRUNC('DAY', fct.PERIOD_DATE) AS DATE,
            AVG(fct.DEMAND_VALUE) AS AVG_DEMAND_MW,
            reg.RESPONDENT AS REGION
        FROM {train_input_table} fct
        JOIN USER_DB_HIPPO.ELECTRICITY_ANALYTICS.DIM_REGION reg 
            ON fct.REGION_KEY = reg.REGION_KEY
        WHERE fct.PERIOD_DATE >= DATEADD(day, -90, TO_TIMESTAMP('{run_date}'))
          AND fct.PERIOD_DATE < TO_TIMESTAMP('{run_date}')
        GROUP BY DATE_TRUNC('DAY', fct.PERIOD_DATE), reg.RESPONDENT
        ORDER BY DATE, REGION
        """
        
        logger.info("Creating training view...")
        cur.execute(create_view_sql)
        
        # Verify view has data
        cur.execute(f"SELECT COUNT(*) FROM {train_view}")
        view_count = cur.fetchone()[0]
        logger.info(f"Training view contains {view_count} records")
        
        if view_count == 0:
            raise ValueError("Training view is empty - cannot train model")
        
        # Create ML model (idempotent)
        create_model_sql = f"""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
            INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
            SERIES_COLNAME => 'REGION',
            TIMESTAMP_COLNAME => 'DATE',
            TARGET_COLNAME => 'AVG_DEMAND_MW',
            CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
        )
        """
        
        logger.info("Training ML model...")
        cur.execute(create_model_sql)
        
        # Show evaluation metrics
        logger.info("Retrieving evaluation metrics...")
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS()")
        metrics = cur.fetchall()
        logger.info(f"Model evaluation metrics: {metrics}")
        
        # COMMIT transaction
        cur.execute("COMMIT")
        logger.info(f"✅ Model {forecast_function_name} trained successfully with {view_count} records")
        
        return {"status": "success", "records": view_count, "run_date": run_date}
        
    except Exception as e:
        # ROLLBACK on error
        logger.error(f"❌ Error training model: {e}")
        try:
            cur.execute("ROLLBACK")
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Rollback failed: {rollback_error}")
        raise
    finally:
        cur.close()


@task
def predict(forecast_function_name, train_input_table, forecast_table, final_table, run_date, **context):
    """
    Generate predictions and create final table (INCREMENTAL)
    - Generate 7-day forecast from run_date
    - Store predictions incrementally (MERGE not REPLACE)
    - Union with historical data for complete view
    - Backfill support: Uses run_date for historical forecasts
    - Idempotent: Can re-run same date without duplicates
    """
    
    # Get Snowflake cursor (inside task, not at parse time)
    cur = return_snowflake_conn()
    
    try:
        # BEGIN transaction
        logger.info(f"Starting forecast generation for run_date: {run_date}")
        cur.execute("BEGIN")
        
        # Generate 7-day forecast
        logger.info("Generating 7-day forecast...")
        forecast_sql = f"""
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        )
        """
        cur.execute(forecast_sql)
        
        # Store predictions to temp table
        logger.info("Storing predictions to temp table...")
        store_predictions_sql = f"""
        CREATE OR REPLACE TABLE {forecast_table} AS 
        SELECT 
            REPLACE(SERIES, '"', '') AS REGION,
            TS AS FORECAST_DATE,
            FORECAST,
            LOWER_BOUND,
            UPPER_BOUND,
            TO_TIMESTAMP('{run_date}') AS GENERATED_AT
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        """
        cur.execute(store_predictions_sql)
        
        # Verify predictions
        cur.execute(f"SELECT COUNT(*) FROM {forecast_table}")
        forecast_count = cur.fetchone()[0]
        logger.info(f"Generated {forecast_count} forecast records")
        
        if forecast_count == 0:
            raise ValueError("No forecasts generated")
        
        # INCREMENTAL MERGE: Insert only new forecasts, update existing ones
        # This makes the load idempotent and supports backfill
        logger.info("Merging forecasts incrementally...")
        merge_forecast_sql = f"""
        MERGE INTO {final_table} AS target
        USING {forecast_table} AS source
        ON target.REGION = source.REGION 
           AND target.FORECAST_DATE = source.FORECAST_DATE
           AND target.GENERATED_AT = source.GENERATED_AT
        WHEN MATCHED THEN
            UPDATE SET
                target.FORECAST = source.FORECAST,
                target.LOWER_BOUND = source.LOWER_BOUND,
                target.UPPER_BOUND = source.UPPER_BOUND
        WHEN NOT MATCHED THEN
            INSERT (REGION, FORECAST_DATE, FORECAST, LOWER_BOUND, UPPER_BOUND, GENERATED_AT)
            VALUES (source.REGION, source.FORECAST_DATE, source.FORECAST, 
                    source.LOWER_BOUND, source.UPPER_BOUND, source.GENERATED_AT)
        """
        cur.execute(merge_forecast_sql)
        
        # Get merge statistics
        cur.execute(f"""
        SELECT COUNT(*) 
        FROM {final_table} 
        WHERE GENERATED_AT = TO_TIMESTAMP('{run_date}')
        """)
        merged_count = cur.fetchone()[0]
        logger.info(f"Merged {merged_count} forecast records for run_date {run_date}")
        
        # Save metrics incrementally (idempotent)
        logger.info("Saving model metrics...")
        save_metrics_sql = f"""
        MERGE INTO USER_DB_HIPPO.ELECTRICITY_ML.MODEL_METRICS AS target
        USING (
            SELECT 
                TO_VARCHAR(TO_TIMESTAMP('{run_date}'), 'YYYYMMDD_HH24MISS') AS MODEL_VERSION,
                TO_TIMESTAMP('{run_date}') AS TRAINING_DATE,
                REGION,
                'FORECAST_PERIODS' AS METRIC_NAME,
                COUNT(*) AS METRIC_VALUE,
                'forecast' AS DATASET_TYPE
            FROM {forecast_table}
            GROUP BY REGION
        ) AS source
        ON target.MODEL_VERSION = source.MODEL_VERSION
           AND target.REGION = source.REGION
           AND target.METRIC_NAME = source.METRIC_NAME
        WHEN MATCHED THEN
            UPDATE SET target.METRIC_VALUE = source.METRIC_VALUE
        WHEN NOT MATCHED THEN
            INSERT (MODEL_VERSION, TRAINING_DATE, REGION, METRIC_NAME, METRIC_VALUE, DATASET_TYPE)
            VALUES (source.MODEL_VERSION, source.TRAINING_DATE, source.REGION, 
                    source.METRIC_NAME, source.METRIC_VALUE, source.DATASET_TYPE)
        """
        cur.execute(save_metrics_sql)
        
        # COMMIT transaction
        cur.execute("COMMIT")
        logger.info(f"✅ Forecast generated and merged incrementally: {merged_count} records")
        
        return {
            "status": "success", 
            "forecast_count": forecast_count,
            "merged_count": merged_count,
            "run_date": run_date
        }
        
    except Exception as e:
        # ROLLBACK on error
        logger.error(f"❌ Error generating forecast: {e}")
        try:
            cur.execute("ROLLBACK")
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Rollback failed: {rollback_error}")
        raise
    finally:
        cur.close()


@task
def ensure_ml_schema_exists():
    """
    Ensure ML schema and tables exist (idempotent)
    - Create schema if not exists
    - Create FORECAST_RESULTS table with proper structure
    - Create MODEL_METRICS table
    - Supports backfill: Can run multiple times safely
    """
    
    # Get Snowflake cursor (inside task, not at parse time)
    cur = return_snowflake_conn()
    
    try:
        # BEGIN transaction
        logger.info("Ensuring ML schema and tables exist...")
        cur.execute("BEGIN")
        
        # Create schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS USER_DB_HIPPO.ELECTRICITY_ML")
        logger.info("Schema ELECTRICITY_ML ready")
        
        # Create FORECAST_RESULTS table (incremental structure)
        create_forecast_table_sql = """
        CREATE TABLE IF NOT EXISTS USER_DB_HIPPO.ELECTRICITY_ML.FORECAST_RESULTS (
            REGION VARCHAR(50) NOT NULL,
            FORECAST_DATE TIMESTAMP NOT NULL,
            ACTUAL FLOAT,
            FORECAST FLOAT,
            LOWER_BOUND FLOAT,
            UPPER_BOUND FLOAT,
            GENERATED_AT TIMESTAMP NOT NULL,
            LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (REGION, FORECAST_DATE, GENERATED_AT)
        )
        """
        cur.execute(create_forecast_table_sql)
        logger.info("Table FORECAST_RESULTS ready")
        
        # Create MODEL_METRICS table
        create_metrics_table_sql = """
        CREATE TABLE IF NOT EXISTS USER_DB_HIPPO.ELECTRICITY_ML.MODEL_METRICS (
            MODEL_VERSION VARCHAR(50) NOT NULL,
            TRAINING_DATE TIMESTAMP NOT NULL,
            REGION VARCHAR(50) NOT NULL,
            METRIC_NAME VARCHAR(100) NOT NULL,
            METRIC_VALUE FLOAT,
            DATASET_TYPE VARCHAR(50),
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (MODEL_VERSION, REGION, METRIC_NAME)
        )
        """
        cur.execute(create_metrics_table_sql)
        logger.info("Table MODEL_METRICS ready")
        
        # COMMIT transaction
        cur.execute("COMMIT")
        logger.info("✅ ML schema and tables verified")
        
        return {"status": "success"}
        
    except Exception as e:
        # ROLLBACK on error
        logger.error(f"❌ Error ensuring schema exists: {e}")
        try:
            cur.execute("ROLLBACK")
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Rollback failed: {rollback_error}")
        raise
    finally:
        cur.close()


# DAG definition (Professor's pattern - no backfill for development)
with DAG(
    dag_id='electricity_ml_forecast',
    start_date=datetime.now() - timedelta(days=1),  # Yesterday - prevents backfill
    catchup=False,  # Disable backfill - only run from now forward
    max_active_runs=1,  # Process one date at a time for safety
    tags=['ML', 'Forecast', 'Daily', 'Production'],
    schedule='0 2 * * *',  # Daily at 2 AM
    default_args={
        'owner': 'data_eng',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(minutes=30),
    }
) as dag:
    
    # Table and model names
    train_input_table = "USER_DB_HIPPO.ELECTRICITY_ANALYTICS.FCT_HOURLY_DEMAND"
    train_view = "USER_DB_HIPPO.ELECTRICITY_ML.DEMAND_FORECAST_VIEW"
    forecast_table = "USER_DB_HIPPO.ELECTRICITY_ML.DEMAND_FORECAST_TEMP"
    forecast_function_name = "USER_DB_HIPPO.ELECTRICITY_ML.PREDICT_ELECTRICITY_DEMAND"
    final_table = "USER_DB_HIPPO.ELECTRICITY_ML.FORECAST_RESULTS"
    
    # Get run date for backfill support (renamed from execution_date to avoid reserved keyword)
    run_date = "{{ ds }}"  # Airflow template variable (YYYY-MM-DD)
    
    # Define tasks (connection created inside each task, not here)
    ensure_schema_task = ensure_ml_schema_exists()
    train_task = train(train_input_table, train_view, forecast_function_name, run_date)
    predict_task = predict(forecast_function_name, train_input_table, forecast_table, final_table, run_date)
    
    # Task dependencies
    ensure_schema_task >> train_task >> predict_task
