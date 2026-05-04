{{
    config(
        materialized='table'
    )
}}

-- Dimension table: Regions
WITH source_regions AS (
    SELECT DISTINCT
        RESPONDENT,
        RESPONDENT_NAME
    FROM {{ ref('stg_eia_hourly_demand') }}
),

region_types AS (
    SELECT
        RESPONDENT,
        RESPONDENT_NAME,
        CASE
            WHEN RESPONDENT = 'US48' THEN 'National'
            ELSE 'Regional'
        END AS REGION_TYPE
    FROM source_regions
)

SELECT
    ROW_NUMBER() OVER (ORDER BY RESPONDENT) AS REGION_KEY,
    RESPONDENT,
    RESPONDENT_NAME,
    REGION_TYPE,
    CURRENT_TIMESTAMP() AS CREATED_AT
FROM region_types
