{{ config(
    materialized='table'
) }}

with features as
(
    select distinct
        car_id,
        case when damaged = 1 then 'yes'
             when damaged = 0 then 'no' end as damaged,
        case when first_owner = 1 then 'yes'
             when first_owner = 0 then 'no' end as first_owner,
        case when personal_using = 1 then 'yes'
             when personal_using = 0 then 'no' end as personal_using,
        case when turbo = 1 then 'yes'
             when turbo = 0 then 'no' end as turbo,
        case when alloy_wheels = 1 then 'yes'
             when alloy_wheels = 0 then 'no' end as alloy_wheels,
        case when adaptive_cruise_control = 1 then 'yes'
             when adaptive_cruise_control = 0 then 'no' end as adaptive_cruise_control,
        case when navigation_system = 1 then 'yes'
             when navigation_system = 0 then 'no' end as navigation_system,
        case when power_liftgate = 1 then 'yes'
             when power_liftgate = 0 then 'no' end as power_liftgate,
        case when backup_camera = 1 then 'yes'
             when backup_camera = 0 then 'no' end as backup_camera,
        case when keyless_start = 1 then 'yes'
             when keyless_start = 0 then 'no' end as keyless_start,
        case when remote_start = 1 then 'yes'
             when remote_start = 0 then 'no' end as remote_start,
        case when sunroof_or_moonroof = 1 then 'yes'
             when sunroof_or_moonroof = 0 then 'no' end as sunroof_or_moonroof,
        case when automatic_emergency_braking = 1 then 'yes'
             when automatic_emergency_braking = 0 then 'no' end as automatic_emergency_braking,
        case when stability_control = 1 then 'yes'
             when stability_control = 0 then 'no' end as stability_control,
        case when leather_seats = 1 then 'yes'
             when leather_seats = 0 then 'no' end as leather_seats,
        case when memory_seat = 1 then 'yes'
             when memory_seat = 0 then 'no' end as memory_seat,
        case when third_row_seating = 1 then 'yes'
             when third_row_seating = 0 then 'no' end as third_row_seating,
        case when apple_car_play_or_android_auto = 1 then 'yes'
             when apple_car_play_or_android_auto = 0 then 'no' end as apple_car_play_or_android_auto,
        case when bluetooth = 1 then 'yes'
             when bluetooth = 0 then 'no' end as bluetooth,
        case when usb_port = 1 then 'yes'
             when usb_port = 0 then 'no' end as usb_port,
        case when heated_seats = 1 then 'yes'
             when heated_seats = 0 then 'no' end as heated_seats
    from landing_01.gcs_used_cars
)
select * from features
