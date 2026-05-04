{{
    config(
        materialized='table'
    )
}}

-- Analytics summary: Aggregated demand metrics for Tableau
WITH fact_demand AS (
    SELECT * FROM {{ ref('fct_hourly_demand') }}
),

dim_region AS (
    SELECT * FROM {{ ref('dim_region') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

summary AS (
    SELECT
        fd.PERIOD_DATE,
        dr.RESPONDENT AS REGION,
        fd.HOUR,
        dd.DAY_OF_WEEK,
        dd.IS_WEEKEND,
        SUM(fd.DEMAND_VALUE) AS TOTAL_DEMAND,
        AVG(fd.DEMAND_VALUE) AS AVG_DEMAND,
        MAX(fd.DEMAND_VALUE) AS MAX_DEMAND,
        MIN(fd.DEMAND_VALUE) AS MIN_DEMAND,
        VARIANCE(fd.DEMAND_VALUE) AS DEMAND_VARIANCE,
        COUNT(*) AS RECORD_COUNT
    FROM fact_demand fd
    LEFT JOIN dim_region dr ON fd.REGION_KEY = dr.REGION_KEY
    LEFT JOIN dim_date dd ON fd.DATE_KEY = dd.DATE_KEY
    GROUP BY fd.PERIOD_DATE, dr.RESPONDENT, fd.HOUR, dd.DAY_OF_WEEK, dd.IS_WEEKEND
)

SELECT
    ROW_NUMBER() OVER (ORDER BY PERIOD_DATE, REGION) AS SUMMARY_KEY,
    PERIOD_DATE,
    REGION,
    HOUR,
    DAY_OF_WEEK,
    IS_WEEKEND,
    TOTAL_DEMAND,
    AVG_DEMAND,
    MAX_DEMAND,
    MIN_DEMAND,
    DEMAND_VARIANCE,
    CURRENT_TIMESTAMP() AS CREATED_AT
FROM summary
