{{ config(
    materialized='table'
) }}

with distinct_fuel_engine as (
    select distinct 
        e.engine_id, 
        f.fuel_id
    from {{ ref('engine') }} e
    ,{{ ref('fuel') }} f
    ,landing_01.gcs_used_cars us
    where (e.engine = us.engine or (us.engine IS NULL AND e.engine IS NULL))
    and (f.fuel_type = us.fuel_type or (us.fuel_type IS NULL AND f.fuel_type IS NULL))
    and {{ filter_last_update() }}
)

select * from distinct_fuel_engine
