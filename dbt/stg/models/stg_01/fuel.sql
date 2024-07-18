{{ config(
    materialized='table'
) }}
with fuel as (
    select distinct dense_rank() over (order by fuel_type) as fuel_id, fuel_type
    from `ready-data-de24.landing_01.gcs_used_cars` 
    where {{ filter_last_update() }}
)
select * from fuel 
