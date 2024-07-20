{{ config(
    materialized='view'
) }}
with dim_car_view as(
  select model,
         min_mpg,
         year,
         fuel_type
  from pl_01.dim_car
)
select * from dim_car_view
