with distinct_modes as (

    select distinct shipping_mode
    from {{ ref('stg_orders') }}

)

select
    row_number() over (order by shipping_mode) as shipping_mode_id,
    shipping_mode
from distinct_modes
