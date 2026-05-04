{{
    config(
        materialized='table'
    )
}}

-- Dimension table: Date
-- Generate date dimension from min to max date in data + 1 year future
WITH date_spine AS (
    SELECT
        DATEADD(day, SEQ4(), '2023-01-01'::DATE) AS DATE_VALUE
    FROM TABLE(GENERATOR(ROWCOUNT => 1461))  -- 4 years
    WHERE DATE_VALUE <= '2026-12-31'
),

date_attributes AS (
    SELECT
        TO_NUMBER(TO_CHAR(DATE_VALUE, 'YYYYMMDD')) AS DATE_KEY,
        DATE_VALUE,
        YEAR(DATE_VALUE) AS YEAR,
        QUARTER(DATE_VALUE) AS QUARTER,
        MONTH(DATE_VALUE) AS MONTH,
        TO_CHAR(DATE_VALUE, 'MMMM') AS MONTH_NAME,
        WEEKOFYEAR(DATE_VALUE) AS WEEK,
        DAY(DATE_VALUE) AS DAY,
        DAYOFWEEK(DATE_VALUE) AS DAY_OF_WEEK,
        TO_CHAR(DATE_VALUE, 'DY') AS DAY_NAME,
        CASE WHEN DAYOFWEEK(DATE_VALUE) IN (0, 6) THEN TRUE ELSE FALSE END AS IS_WEEKEND,
        FALSE AS IS_HOLIDAY  -- Can be enhanced with holiday calendar
    FROM date_spine
)

SELECT * FROM date_attributes
