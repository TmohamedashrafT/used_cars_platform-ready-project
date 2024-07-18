{{ config(
    materialized='table'
) }}
with color as(
    SELECT DISTINCT DENSE_RANK() OVER (ORDER BY color) as color_id, color
    FROM (
    SELECT interior_color AS color FROM `ready-data-de24.landing_01.gcs_used_cars` where {{ filter_last_update() }}
    UNION ALL
    SELECT exterior_color AS color FROM `ready-data-de24.landing_01.gcs_used_cars` where {{ filter_last_update() }}
    ) 
)
select * from color