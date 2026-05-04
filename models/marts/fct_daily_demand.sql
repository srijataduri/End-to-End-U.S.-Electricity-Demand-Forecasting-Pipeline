{{ config(materialized='table') }}

with hourly as (

    select
        period_date,
        respondent,
        respondent_name,
        demand_value
    from {{ ref('stg_eia_hourly_demand') }}

),

daily_base as (

    select
        date_trunc('day', period_date) as period_day,
        respondent,
        respondent_name,
        demand_value
    from hourly

),

daily as (

    select
        period_day,
        respondent,
        respondent_name,
        avg(demand_value) as avg_demand_mw,
        min(demand_value) as min_demand_mw,
        max(demand_value) as max_demand_mw,
        sum(demand_value) as total_demand_mwh,
        count(*) as hourly_record_count
    from daily_base
    group by
        period_day,
        respondent,
        respondent_name

)

select *
from daily
