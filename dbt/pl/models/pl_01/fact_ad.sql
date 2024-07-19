{{ config(
    materialized='incremental',
) }}

with fact_ad as 
(
select  
    dc.car_key, 
    dj.junk_key, 
    idc.color_key as interior_color_key, 
    xdc.color_key as exterior_color_key,
    COALESCE(c.mileage, 0) as mileage ,
    COALESCE(c.price, 0) as price
from stg_01.car c
left join {{ ref('dim_car')}} dc 
    on COALESCE(c.year, -1) = dc.year and
    COALESCE(c.min_mpg, -1) = dc.min_mpg and
    COALESCE(c.max_mpg, -1) = dc.max_mpg and 
    dc.model_id = c.model_id and
    dc.transmission_id = c.transmission_id and  
    dc.engine_id = c.engine_id and 
    dc.fuel_id = c.fuel_id
left join {{ ref('dim_color')}} idc
    on idc.color_id = c.interior_color_id 
left join {{ ref('dim_color') }} xdc
    on xdc.color_id = c.exterior_color_id
left join stg_01.drivetrain dr
    on dr.drivetrain_id = c.drivetrain_id
left join stg_01.features f 
    on f.car_id = c.car_id
left join stg_01.fuel fu
    on fu.fuel_id = c.fuel_id 
left join stg_01.transmission tr 
    on tr.transmission_id = c.transmission_id
left join {{ ref('dim_junk')}} dj
    on dj.damaged = COALESCE(f.damaged, 'unknown') and
     dj.first_owner = COALESCE(f.first_owner, 'unknown') and
     dj.personal_using = COALESCE(f.personal_using, 'unknown') and
     dj.turbo = COALESCE(f.turbo, 'unknown') and
     dj.alloy_wheels = COALESCE(f.alloy_wheels, 'unknown') and
     dj.adaptive_cruise_control = COALESCE(f.adaptive_cruise_control, 'unknown') and
     dj.navigation_system = COALESCE(f.navigation_system, 'unknown') and
     dj.power_liftgate = COALESCE(f.power_liftgate, 'unknown') and
     dj.backup_camera = COALESCE(f.backup_camera, 'unknown') and
     dj.keyless_start = COALESCE(f.keyless_start, 'unknown') and
     dj.remote_start = COALESCE(f.remote_start, 'unknown') and
     dj.sunroof_or_moonroof = COALESCE(f.sunroof_or_moonroof, 'unknown') and
     dj.automatic_emergency_braking = COALESCE(f.automatic_emergency_braking, 'unknown') and
     dj.stability_control = COALESCE(f.stability_control, 'unknown') and
     dj.leather_seats = COALESCE(f.leather_seats, 'unknown') and
     dj.memory_seat = COALESCE(f.memory_seat, 'unknown') and
     dj.third_row_seating = COALESCE(f.third_row_seating, 'unknown') and
     dj.apple_car_play_or_android_auto = COALESCE(f.apple_car_play_or_android_auto, 'unknown') and  
     dj.bluetooth = COALESCE(f.bluetooth, 'unknown') and 
     dj.usb_port = COALESCE(f.usb_port, 'unknown') and 
     dj.heated_seats = COALESCE(f.heated_seats, 'unknown') and 
     dj.drivetrain = COALESCE(dr.drivetrain, 'unknown') and
     dj.automatic_transmission = COALESCE(tr.automatic_transmission, -1)
    
)
select * from fact_ad