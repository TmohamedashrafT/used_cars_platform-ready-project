{{ config(
    materialized='table'
) }}
with engine  as
(
    select  distinct dense_rank() over (order by engine) as engine_id, engine, engine_size
    from `ready-data-de24.landing_01.gcs_used_cars` 
    where {{ filter_last_update() }}
)
select * from engine 