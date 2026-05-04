{{
    config(
        materialized='view'
    )
}}

-- Staging model: Aggregate demand by region and hour
WITH hourly_demand AS (
    SELECT * FROM {{ ref('stg_eia_hourly_demand') }}
),

aggregated AS (
    SELECT
        PERIOD_DATE,
        RESPONDENT,
        RESPONDENT_NAME,
        SUM(DEMAND_VALUE) AS TOTAL_DEMAND,
        AVG(DEMAND_VALUE) AS AVG_DEMAND,
        MAX(DEMAND_VALUE) AS MAX_DEMAND,
        MIN(DEMAND_VALUE) AS MIN_DEMAND,
        COUNT(*) AS RECORD_COUNT
    FROM hourly_demand
    GROUP BY PERIOD_DATE, RESPONDENT, RESPONDENT_NAME
)

SELECT * FROM aggregated
