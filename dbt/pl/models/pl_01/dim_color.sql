{{ config(
    materialized='incremental',
) }}



with dim_color as (

   select   row_number() OVER  (ORDER BY c.color_id) + coalesce((select max(color_key) from pl_01.dim_color), 1) as color_key,
   c.color_id,
   COALESCE(c.color, 'unknown') as color
   from stg_01.color c
   where not exists
   (
    select 1 
    from pl_01.dim_color ex
    where ex.color = COALESCE(c.color, 'unknown')
    )

)
select * from dim_color
