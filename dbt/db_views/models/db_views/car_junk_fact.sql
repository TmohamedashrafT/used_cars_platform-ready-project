{{ config(
    materialized='view'
) }}

with car_junk_fact_view as(
  select model,
         mileage,
         year,
         automatic_transmission,
         price,
         fuel_type
  from pl_01.fact_ad fa 
  join pl_01.dim_car dc 
  on fa.car_key = dc.car_key
  join pl_01.dim_junk dj
  on dj.junk_key = fa.junk_key
)
select * from car_junk_fact_view