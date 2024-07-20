{{ config(
    materialized='view'
) }}
with dim_car_and_fact_view as(
    select brand,  
    from pl_01.fact_ad f  join `pl_01.dim_car` c
    on f.car_key = c.car_key
)
select * from dim_car_and_fact_view