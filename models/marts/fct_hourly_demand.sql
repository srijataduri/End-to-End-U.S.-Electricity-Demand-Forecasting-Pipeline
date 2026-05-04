{{
    config(
        materialized='table'
    )
}}

-- Fact table: Hourly electricity demand
WITH staging_demand AS (
    SELECT * FROM {{ ref('stg_eia_hourly_demand') }}
),

dim_region AS (
    SELECT * FROM {{ ref('dim_region') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

fact_data AS (
    SELECT
        sd.PERIOD_DATE,
        dd.DATE_KEY,
        dr.REGION_KEY,
        sd.HOUR,
        sd.DEMAND_VALUE,
        sd.VALUE_UNITS AS DEMAND_UNITS,
        sd.LOADED_AT
    FROM staging_demand sd
    LEFT JOIN dim_region dr
        ON sd.RESPONDENT = dr.RESPONDENT
    LEFT JOIN dim_date dd
        ON sd.DATE_VALUE = dd.DATE_VALUE
)

SELECT
    ROW_NUMBER() OVER (ORDER BY PERIOD_DATE, REGION_KEY) AS DEMAND_KEY,
    PERIOD_DATE,
    DATE_KEY,
    REGION_KEY,
    HOUR,
    DEMAND_VALUE,
    DEMAND_UNITS,
    LOADED_AT
FROM fact_data
