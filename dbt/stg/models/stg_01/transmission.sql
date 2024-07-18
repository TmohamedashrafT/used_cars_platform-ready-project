{{ config(
    materialized='table'
) }}

with transmission as
( select distinct dense_rank() over (order by transmission) as transmission_id, transmission, automatic_transmission
    from `ready-data-de24.landing_01.gcs_used_cars`
    where {{ filter_last_update() }}
)
select * from transmission