{{ config(
    materialized='table'
) }}

with model as (
    select distinct 
        dense_rank() over (order by uc.model) as model_id,
        uc.model, 
        b.brand_id
    from `ready-data-de24.landing_01.gcs_used_cars` uc
    join {{ ref('brand') }} b
        on uc.brand = b.brand or (uc.brand IS NULL AND b.brand IS NULL)
    where {{ filter_last_update() }}
)

select * from model
