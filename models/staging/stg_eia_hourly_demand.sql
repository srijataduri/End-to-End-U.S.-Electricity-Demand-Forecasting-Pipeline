{{
    config(
        materialized='view'
    )
}}

-- Staging model: Clean and standardize raw EIA data
WITH source AS (
    SELECT
        PERIOD_DATE,
        RESPONDENT,
        RESPONDENT_NAME,
        DATA_TYPE,
        DATA_TYPE_NAME,
        VALUE,
        VALUE_UNITS,
        LOADED_AT
    FROM {{ source('electricity_raw', 'EIA_HOURLY_DEMAND') }}
),

cleaned AS (
    SELECT
        PERIOD_DATE,
        RESPONDENT,
        RESPONDENT_NAME,
        DATA_TYPE,
        DATA_TYPE_NAME,
        VALUE AS DEMAND_VALUE,
        VALUE_UNITS,
        LOADED_AT,
        -- Extract time components
        EXTRACT(HOUR FROM PERIOD_DATE) AS HOUR,
        EXTRACT(DAY FROM PERIOD_DATE) AS DAY,
        EXTRACT(MONTH FROM PERIOD_DATE) AS MONTH,
        EXTRACT(YEAR FROM PERIOD_DATE) AS YEAR,
        DAYOFWEEK(PERIOD_DATE) AS DAY_OF_WEEK,
        DATE_TRUNC('DAY', PERIOD_DATE) AS DATE_VALUE
    FROM source
    WHERE VALUE IS NOT NULL
      AND VALUE > 0  -- Demand must be positive
      AND PERIOD_DATE IS NOT NULL
)

SELECT * FROM cleaned
