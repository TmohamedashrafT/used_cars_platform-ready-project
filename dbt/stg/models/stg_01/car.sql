{{ config(
    materialized='table'
) }}

with car as (
  
SELECT 
    us.car_id,
    us.year, 
    en.engine_id, 
    dr.drivetrain_id, 
    co_in.color_id AS interior_color_id,
    co_ex.color_id AS exterior_color_id,
    tr.transmission_id,
    mo.model_id,
    us.mileage,
    us.price,
    us.min_mpg,
    us.max_mpg,
    fu.fuel_id
FROM 
    landing_01.gcs_used_cars us
JOIN {{ ref('engine') }} en
    ON (us.engine = en.engine OR (us.engine IS NULL AND en.engine IS NULL)) 
    AND (us.engine_size = en.engine_size OR (us.engine_size IS NULL AND en.engine_size IS NULL))
JOIN {{ ref('drivetrain') }} dr
    ON us.drivetrain = dr.drivetrain OR (us.drivetrain IS NULL AND dr.drivetrain IS NULL)
JOIN {{ ref('color') }} co_in
    ON us.interior_color = co_in.color OR (us.interior_color IS NULL AND co_in.color IS NULL)
JOIN {{ ref('color') }} co_ex
    ON us.exterior_color = co_ex.color OR (us.exterior_color IS NULL AND co_ex.color IS NULL)
JOIN {{ ref('transmission') }} tr
    ON (us.transmission = tr.transmission OR (us.transmission IS NULL AND tr.transmission IS NULL)) 
    AND (us.automatic_transmission = tr.automatic_transmission OR (us.automatic_transmission IS NULL AND tr.automatic_transmission IS NULL))
JOIN {{ ref('model') }} mo
    ON us.model = mo.model OR (us.model IS NULL AND mo.model IS NULL)
JOIN {{ ref('fuel') }} fu
    ON us.fuel_type = fu.fuel_type OR (us.fuel_type IS NULL AND fu.fuel_type IS NULL)
WHERE 
    {{ filter_last_update() }}
)

select * from car
