{{ config(
    materialized='table'
) }}
with brand as (

   select  distinct dense_rank() over (order by brand) as brand_id,
   brand
   from `ready-data-de24.landing_01.gcs_used_cars` 
   where {{ filter_last_update() }}

)
select * from brand
