{{ config(
    materialized='incremental',
) }}
with features as
(
    select distinct 
    COALESCE(t.automatic_transmission, -1) as automatic_transmission,
    COALESCE(d.drivetrain, 'unknown') as drivetrain,
    COALESCE(fu.fuel_type, 'unknown') as fuel_type,
    COALESCE(f.damaged , 'unknown') as damaged ,
    COALESCE(f.first_owner , 'unknown') as first_owner ,
    COALESCE(f.personal_using, 'unknown') as personal_using,
    COALESCE(f.turbo , 'unknown') as turbo ,
    COALESCE(f.alloy_wheels , 'unknown') as alloy_wheels ,
    COALESCE(f.adaptive_cruise_control , 'unknown') as adaptive_cruise_control ,
    COALESCE(f.navigation_system , 'unknown') as navigation_system ,
    COALESCE(f.power_liftgate , 'unknown') as power_liftgate ,
    COALESCE(f.backup_camera , 'unknown') as backup_camera ,
    COALESCE(f.keyless_start , 'unknown') as keyless_start ,
    COALESCE(f.remote_start , 'unknown') as remote_start ,
    COALESCE(f.sunroof_or_moonroof , 'unknown') as sunroof_or_moonroof ,
    COALESCE(f.automatic_emergency_braking , 'unknown') as automatic_emergency_braking ,
    COALESCE(f.stability_control , 'unknown') as stability_control ,
    COALESCE(f.leather_seats , 'unknown') as leather_seats ,
    COALESCE(f.memory_seat , 'unknown') as memory_seat ,
    COALESCE(f.third_row_seating , 'unknown') as third_row_seating ,
    COALESCE(f.apple_car_play_or_android_auto , 'unknown') as apple_car_play_or_android_auto ,
    COALESCE(f.bluetooth , 'unknown') as bluetooth,
    COALESCE(f.usb_port , 'unknown') as usb_port ,
    COALESCE(f.heated_seats , 'unknown') as heated_seats
    from stg_01.car c
    left join stg_01.features f on f.car_id = c.car_id
    left join stg_01.engine e on c.engine_id = e.engine_id
    left join stg_01.fuel_engine fe on fe.engine_id = e.engine_id
    left join stg_01.fuel fu on fu.fuel_id = fe.fuel_id
    left join stg_01.transmission t on t.transmission_id = c.transmission_id
    left join stg_01.drivetrain d on d.drivetrain_id = c.drivetrain_id
),
dim_junk as
(
    select row_number() OVER () + coalesce((select max(junk_key) from pl_01.dim_junk), 1) as junk_key,
    *
    from features f

        where not exists (
        select 1
        from pl_01.dim_junk jd
        where jd.automatic_transmission = COALESCE(f.automatic_transmission, -1)
          and jd.drivetrain = COALESCE(f.drivetrain, 'unknown')
          and jd.fuel_type = COALESCE(f.fuel_type, 'unknown')
          and jd.damaged = COALESCE(f.damaged, 'unknown')
          and jd.first_owner = COALESCE(f.first_owner, 'unknown')
          and jd.personal_using = COALESCE(f.personal_using, 'unknown')
          and jd.turbo = COALESCE(f.turbo, 'unknown')
          and jd.alloy_wheels = COALESCE(f.alloy_wheels, 'unknown')
          and jd.adaptive_cruise_control = COALESCE(f.adaptive_cruise_control, 'unknown')
          and jd.navigation_system = COALESCE(f.navigation_system, 'unknown')
          and jd.power_liftgate = COALESCE(f.power_liftgate, 'unknown')
          and jd.backup_camera = COALESCE(f.backup_camera, 'unknown')
          and jd.keyless_start = COALESCE(f.keyless_start, 'unknown')
          and jd.remote_start = COALESCE(f.remote_start, 'unknown')
          and jd.sunroof_or_moonroof = COALESCE(f.sunroof_or_moonroof, 'unknown')
          and jd.automatic_emergency_braking = COALESCE(f.automatic_emergency_braking, 'unknown')
          and jd.stability_control = COALESCE(f.stability_control, 'unknown')
          and jd.leather_seats = COALESCE(f.leather_seats, 'unknown')
          and jd.memory_seat = COALESCE(f.memory_seat, 'unknown')
          and jd.third_row_seating = COALESCE(f.third_row_seating, 'unknown')
          and jd.apple_car_play_or_android_auto = COALESCE(f.apple_car_play_or_android_auto, 'unknown')
          and jd.bluetooth = COALESCE(f.bluetooth, 'unknown')
          and jd.usb_port = COALESCE(f.usb_port, 'unknown')
          and jd.heated_seats = COALESCE(f.heated_seats, 'unknown')
    )
)
select * from dim_junk
