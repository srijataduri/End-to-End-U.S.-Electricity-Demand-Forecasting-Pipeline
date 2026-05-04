{% snapshot snap_dim_region %}

{{
    config(
      target_schema='ELECTRICITY_ANALYTICS',
      unique_key='RESPONDENT',
      strategy='check',
      check_cols='all',
    )
}}

-- Snapshot of DIM_REGION for change tracking (SCD Type 2)
-- Tracks when region names or attributes change over time
SELECT 
    RESPONDENT,
    RESPONDENT_NAME,
    'Regional' AS REGION_TYPE,
    CURRENT_TIMESTAMP() AS CREATED_AT
FROM {{ source('electricity_raw', 'EIA_HOURLY_DEMAND') }}
WHERE RESPONDENT IS NOT NULL
GROUP BY RESPONDENT, RESPONDENT_NAME

{% endsnapshot %}
