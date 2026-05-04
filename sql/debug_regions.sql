-- 1. Check raw data - should have 6 regions
SELECT 
    RESPONDENT,
    COUNT(*) as raw_count,
    MIN(PERIOD_DATE) as earliest_date,
    MAX(PERIOD_DATE) as latest_date
FROM USER_DB_HIPPO.ELECTRICITY_RAW.EIA_HOURLY_DEMAND
GROUP BY RESPONDENT
ORDER BY RESPONDENT;

-- 2. Check staging view - any filtering happening?
SELECT 
    RESPONDENT,
    COUNT(*) as staging_count
FROM USER_DB_HIPPO.ELECTRICITY_ANALYTICS.STG_EIA_HOURLY_DEMAND
GROUP BY RESPONDENT
ORDER BY RESPONDENT;

-- 3. Check dimension table - should have 6 regions
SELECT 
    REGION_KEY,
    RESPONDENT,
    RESPONDENT_NAME,
    REGION_TYPE
FROM USER_DB_HIPPO.ELECTRICITY_ANALYTICS.DIM_REGION
ORDER BY RESPONDENT;

-- 4. Check fact table - are all 6 regions making it through?
SELECT 
    reg.RESPONDENT,
    COUNT(*) as fact_count,
    MIN(fct.PERIOD_DATE) as earliest_date,
    MAX(fct.PERIOD_DATE) as latest_date
FROM USER_DB_HIPPO.ELECTRICITY_ANALYTICS.FCT_HOURLY_DEMAND fct
JOIN USER_DB_HIPPO.ELECTRICITY_ANALYTICS.DIM_REGION reg
    ON fct.REGION_KEY = reg.REGION_KEY
GROUP BY reg.RESPONDENT
ORDER BY reg.RESPONDENT;

-- 5. Check ML training view - which regions are being trained?
SELECT 
    REGION,
    COUNT(*) as training_records,
    MIN(DATE) as earliest_date,
    MAX(DATE) as latest_date
FROM USER_DB_HIPPO.ELECTRICITY_ML.DEMAND_FORECAST_VIEW
GROUP BY REGION
ORDER BY REGION;

-- 6. Check ML forecasts - which regions have predictions?
SELECT 
    REGION,
    COUNT(*) as forecast_count,
    MIN(FORECAST_DATE) as first_forecast,
    MAX(FORECAST_DATE) as last_forecast
FROM USER_DB_HIPPO.ELECTRICITY_ML.FORECAST_RESULTS
GROUP BY REGION
ORDER BY REGION;
