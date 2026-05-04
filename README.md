# Electricity Demand Forecasting - End-to-End Data Pipeline

**SJSU Data Engineering Graduate Project**  
**Objective**: Production-ready pipeline for electricity demand forecasting using real-time EIA data

## 🎯 Project Overview

Complete data engineering solution that:
- Fetches hourly electricity demand data from EIA API (4 US regions: US48, CISO, MISO, PJM)
- Processes ~670 records per hourly run (7 days × 24 hours × 4 regions)
- Accumulates 200k+ historical records in Snowflake data warehouse
- Transforms data using dbt into analytics-ready dimensional model
- Generates 7-day forecasts using Snowflake ML FORECAST
- Ready for Preset visualization

## 🏗️ Architecture

```
EIA API → Airflow ETL → Snowflake (Raw) → dbt Transform → 
Snowflake (Analytics) → Snowflake ML → Preset Dashboard
```

**Data Flow:**
1. **ETL DAG** (hourly at :00): Fetch from EIA API → Load to `ELECTRICITY_RAW`
2. **dbt DAG** (hourly at :30): Transform → Load to `ELECTRICITY_ANALYTICS`
3. **ML DAG** (daily at 2 AM): Train model → Generate forecasts → Store in `ELECTRICITY_ML`

## 📊 Technology Stack

- **Orchestration**: Apache Airflow 2.10.1 (Docker, LocalExecutor)
- **Data Warehouse**: Snowflake (3 schemas, 10+ tables)
- **Transformation**: dbt 1.8.8 with dbt-snowflake 1.8.4
- **ML**: Snowflake ML FORECAST (built-in time series forecasting)
- **Visualization**: Preset
- **Language**: Python 3.12
- **API**: EIA v2 (US Energy Information Administration)

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed and running
- Snowflake account with TRAINING_ROLE access
- EIA API key (free from https://www.eia.gov/opendata/register.php)

### 1. Clone Repository
```bash
git clone <your-repo>
cd sjsu-data226-electricity-forecast
```

### 2. Configure Environment
Edit `.env` file with your credentials:
```bash
# Snowflake
SNOWFLAKE_ACCOUNT=sfedu02-lvb17920
SNOWFLAKE_USER=HIPPO
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=USER_DB_HIPPO
SNOWFLAKE_WAREHOUSE=HIPPO_QUERY_WH
SNOWFLAKE_ROLE=TRAINING_ROLE

# EIA API
EIA_API_KEY=your_eia_api_key
```

### 3. Setup Snowflake Schemas
Run in Snowflake worksheet:
```sql
-- Create schemas
CREATE SCHEMA IF NOT EXISTS USER_DB_HIPPO.ELECTRICITY_RAW;
CREATE SCHEMA IF NOT EXISTS USER_DB_HIPPO.ELECTRICITY_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS USER_DB_HIPPO.ELECTRICITY_ML;
```

### 4. Start Airflow
```bash
# Start containers
docker compose up -d

# Wait 2-3 minutes for initialization
# Access Airflow UI: http://localhost:8081
# Username: airflow | Password: airflow
```

### 5. Configure Airflow Connection

In Airflow UI → Admin → Connections → Add Connection:
- **Connection Id**: `snowflake_conn`
- **Connection Type**: Snowflake
- **Account**: `sfedu02-lvb17920`
- **Login**: `HIPPO`
- **Password**: (your password)
- **Schema**: `ELECTRICITY_RAW`
- **Extra**: 
```json
{
  "account": "sfedu02-lvb17920",
  "database": "USER_DB_HIPPO",
  "warehouse": "HIPPO_QUERY_WH",
  "role": "TRAINING_ROLE"
}
```

### 6. Run Pipeline

**Option A: Run All DAGs Manually (First Time)**
1. Trigger `electricity_eia_etl_live` → Wait for completion (~2 min)
2. Trigger `electricity_dbt_transform` → Wait for completion (~1 min)
3. Trigger `electricity_ml_forecast` → Wait for completion (~2 min)

**Option B: Let Schedule Run Automatically**
- ETL runs hourly at :00
- dbt runs hourly at :30 (30 min after ETL)
- ML runs daily at 2:00 AM

## 📁 Project Structure

```
sjsu-data226-electricity-forecast/
├── airflow/
│   ├── dags/
│   │   ├── electricity_eia_etl_live.py      # ETL: EIA API → Snowflake
│   │   ├── electricity_dbt_transform.py     # dbt: Transform data
│   │   └── electricity_ml_forecast.py       # ML: Snowflake ML FORECAST
│   ├── logs/                                 # Airflow logs
│   └── plugins/                              # Custom plugins (empty)
├── dbt/
│   ├── electricity_forecast/
│   │   ├── models/
│   │   │   ├── staging/                     # Staging views
│   │   │   │   ├── stg_eia_hourly_demand.sql
│   │   │   │   └── stg_demand_by_region.sql
│   │   │   └── marts/                       # Analytics tables
│   │   │       ├── dim_region.sql           # Region dimension
│   │   │       ├── dim_date.sql             # Date dimension
│   │   │       ├── fct_hourly_demand.sql    # Fact table
│   │   │       └── analytics_demand_summary.sql
│   │   ├── snapshots/
│   │   │   └── snap_dim_region.sql          # SCD Type 2
│   │   └── dbt_project.yml
│   └── profiles.yml                          # dbt connection config
├── sql/
│   ├── create_snowflake_schemas.sql         # Schema setup
│   └── INITIAL_SETUP.sql                    # Initial data load
├── reports/
│   ├── FINAL_REPORT_TEMPLATE.md             # Report template
│   └── PRESENTATION_OUTLINE.md              # Presentation guide
├── docker-compose.yaml                       # Airflow setup
├── requirements.txt                          # Python dependencies
├── .env                                      # Environment variables
└── README.md                                 # This file
```

## 🗄️ Database Schema

### ELECTRICITY_RAW (Source Data)
- `EIA_HOURLY_DEMAND`: Raw API data (~200k records)
  - Columns: PERIOD_DATE, RESPONDENT, RESPONDENT_NAME, VALUE, LOADED_AT

### ELECTRICITY_ANALYTICS (Transformed Data)
- `STG_EIA_HOURLY_DEMAND`: Staging view (cleaned data)
- `DIM_REGION`: Region dimension (4 regions: US48, CISO, MISO, PJM)
- `DIM_DATE`: Date dimension (4 years: 2023-2026)
- `FCT_HOURLY_DEMAND`: Fact table (hourly demand by region)
- `ANALYTICS_DEMAND_SUMMARY`: Pre-aggregated metrics for Preset

### ELECTRICITY_ML (ML Outputs)
- `FORECAST_RESULTS`: 7-day forecasts (28 records: 7 days × 4 regions)
  - Columns: REGION, FORECAST_DATE, FORECAST, LOWER_BOUND, UPPER_BOUND, GENERATED_AT
- `MODEL_METRICS`: Model performance metrics
- `DEMAND_FORECAST_VIEW`: Training data view (90 days)

## 🔄 DAG Details

### 1. electricity_eia_etl_live
**Schedule**: Hourly at :00 (e.g., 1:00, 2:00, 3:00)  
**Runtime**: ~2 minutes  
**Tasks**:
- `create_temp_table`: Create staging table
- `extract_load_*`: Fetch data for 6 regions in parallel
- `merge_to_main`: MERGE into main table (idempotent)
- `cleanup`: Drop temp table

**Features**:
- Parallel extraction (6 regions simultaneously)
- Idempotent: Can re-run safely (MERGE not INSERT)
- Transaction safety: BEGIN/COMMIT/ROLLBACK
- Error handling: Try/except/raise

### 2. electricity_dbt_transform
**Schedule**: Hourly at :30 (30 min after ETL)  
**Runtime**: ~1 minute  
**Tasks**:
- `dbt_deps`: Install dbt packages
- `dbt_transform`: Run all models (staging → marts)
- `dbt_test`: Run 33 data quality tests
- `dbt_snapshot`: Capture dimension changes (SCD Type 2)
- `dbt_document`: Generate documentation

**Features**:
- Incremental models (efficient updates)
- 33 data quality tests (not_null, unique, relationships)
- Idempotent: Can re-run safely

### 3. electricity_ml_forecast
**Schedule**: Daily at 2:00 AM  
**Runtime**: ~2 minutes  
**Tasks**:
- `ensure_ml_schema_exists`: Create schema and tables
- `train`: Train Snowflake ML FORECAST model (90 days of data)
- `predict`: Generate 7-day forecasts

**Features**:
- Uses Snowflake ML FORECAST (automatic feature engineering)
- Generates prediction intervals (95% confidence)
- Idempotent: MERGE forecasts (not INSERT)

## 🧪 Verification

### Check Data Counts
```sql
-- Raw data (should be ~200k)
SELECT COUNT(*) FROM USER_DB_HIPPO.ELECTRICITY_RAW.EIA_HOURLY_DEMAND;

-- Analytics data
SELECT COUNT(*) FROM USER_DB_HIPPO.ELECTRICITY_ANALYTICS.FCT_HOURLY_DEMAND;
SELECT COUNT(*) FROM USER_DB_HIPPO.ELECTRICITY_ANALYTICS.DIM_REGION;  -- Should be 4
SELECT COUNT(*) FROM USER_DB_HIPPO.ELECTRICITY_ANALYTICS.DIM_DATE;    -- Should be 1461

-- ML forecasts (should be 28: 7 days × 4 regions)
SELECT COUNT(*) FROM USER_DB_HIPPO.ELECTRICITY_ML.FORECAST_RESULTS;
```

### View Sample Forecasts
```sql
SELECT 
    REGION,
    FORECAST_DATE,
    FORECAST,
    LOWER_BOUND,
    UPPER_BOUND
FROM USER_DB_HIPPO.ELECTRICITY_ML.FORECAST_RESULTS
ORDER BY REGION, FORECAST_DATE
LIMIT 10;
```

## 🐛 Troubleshooting

### Airflow Not Starting
```bash
# Check logs
docker compose logs airflow

# Restart
docker compose down
docker compose up -d
```

### DAG Not Showing in UI
- Wait 2-3 minutes after startup
- Check for Python syntax errors in DAG files
- Check Airflow logs: `docker compose logs airflow | grep ERROR`

### Snowflake Connection Failed
- Verify credentials in `.env`
- Check Airflow connection configuration
- Test connection in Snowflake worksheet

### dbt Models Failing
```bash
# Check dbt logs in Airflow UI
# Or run manually:
docker compose exec airflow bash
cd /opt/airflow/dbt/electricity_forecast
dbt run --profiles-dir /opt/airflow/dbt
```


## 📚 References

- [EIA API Documentation](https://www.eia.gov/opendata/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Airflow Documentation](https://airflow.apache.org/)
- [Snowflake ML FORECAST](https://docs.snowflake.com/en/user-guide/ml-powered-forecasting)

## 📧 Support

For questions or issues:
- Review Airflow logs in UI
- Contact: [srija.taduri@sjsu.edu]
