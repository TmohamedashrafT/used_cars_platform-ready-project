{{ config(
    materialized='incremental',
) }}

with car as (
    select distinct 
    m.brand_id,
    COALESCE(b.brand, 'unknown') as brand,
    c.model_id,
    COALESCE(m.model, 'unknown') as model,
    COALESCE(c.year, -1) as year,
    c.engine_id,
    COALESCE(e.engine, 'unknown') as engine,
    COALESCE(e.engine_size, -1) as engine_size,
    c.transmission_id,
    COALESCE(t.transmission, 'unknown') as transmission,
    COALESCE(c.min_mpg, -1) as min_mpg,
    COALESCE(c.max_mpg, -1) as max_mpg,
    c.fuel_id,
    COALESCE(f.fuel_type, 'unknown') as fuel_type

   from   `ready-data-de24.stg_01.car` c
   left join stg_01.model m on m.model_id = c.model_id
   left join stg_01.brand b on b.brand_id = m.brand_id
   left join stg_01.transmission t on t.transmission_id = c.transmission_id
   left join stg_01.engine e on e.engine_id = c.engine_id 
   left join stg_01.fuel f on f.fuel_id = c.fuel_id 
    where not exists(
    select 1 
    from pl_01.dim_car ex
    where 
    COALESCE(b.brand, 'unknown')  = ex.brand and
    COALESCE(m.model, 'unknown')  = ex.model and
    COALESCE(e.engine, 'unknown') =ex.engine and
    COALESCE(t.transmission, 'unknown') = ex.transmission and
    COALESCE(c.min_mpg, -1) = ex.min_mpg and
    COALESCE(c.max_mpg, -1) = ex.max_mpg
   )
),

dim_car as (

   select   row_number() OVER() + coalesce((select max(car_key) from pl_01.dim_car), 1) as car_key,
    *
   from car    
)
select * from dim_car
