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
    us.max_mpg
FROM 
		 landing_01.gcs_used_cars us, 
        {{ ref('engine') }} en,
        {{ ref('drivetrain') }} dr,
        {{ ref('color') }} co_in,
        {{ ref('color') }} co_ex,
        {{ ref('transmission') }} tr,
        {{ ref('model') }} mo
WHERE 
    ((us.engine = en.engine OR (us.engine IS NULL AND en.engine IS NULL)) AND 
     (us.engine_size = en.engine_size OR (us.engine_size IS NULL AND en.engine_size IS NULL))) AND
    (us.drivetrain = dr.drivetrain OR (us.drivetrain IS NULL AND dr.drivetrain IS NULL)) AND
    (us.interior_color = co_in.color OR (us.interior_color IS NULL AND co_in.color IS NULL)) AND
    (us.exterior_color = co_ex.color OR (us.exterior_color IS NULL AND co_ex.color IS NULL)) AND
    (us.transmission = tr.transmission OR (us.transmission IS NULL AND tr.transmission IS NULL)) AND
    (us.automatic_transmission = tr.automatic_transmission OR (us.automatic_transmission IS NULL AND tr.automatic_transmission IS NULL)) AND
    (us.model = mo.model OR (us.model IS NULL AND mo.model IS NULL)) AND
	{{ filter_last_update() }}
)

select * from car
