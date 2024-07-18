{{ config(
    materialized='table'
) }}
with drivetrain as 
(
    select  distinct dense_rank() over (order by drivetrain) as drivetrain_id, drivetrain
    from `ready-data-de24.landing_01.gcs_used_cars`  
    where {{ filter_last_update() }}
)
select * from drivetrain