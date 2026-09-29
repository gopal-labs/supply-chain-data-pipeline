with source as (

    select * from {{ source('raw', 'orders_raw') }}
    where batch_date is not null

)

select
    order_item_id::int                     as order_item_id,
    order_id::int                          as order_id,
    order_customer_id::int                 as order_customer_id,
    customer_id::int                       as customer_id,

    order_date::timestamp                  as order_date,
    shipping_date::timestamp               as shipping_date,
    days_for_shipping_real::int            as days_for_shipping_real,
    days_for_shipment_scheduled::int       as days_for_shipment_scheduled,

    delivery_status,
    (late_delivery_risk = '1')             as late_delivery_risk,

    category_id::int                       as category_id,
    category_name,

    customer_city,
    customer_country,
    customer_state,
    customer_segment,
    customer_zipcode,

    department_id::int                     as department_id,
    department_name,

    latitude::numeric                      as latitude,
    longitude::numeric                     as longitude,

    market,
    order_city,
    order_country,
    order_region,
    order_state,
    order_status,
    order_zipcode,

    order_item_cardprod_id::int            as order_item_cardprod_id,
    order_item_discount::numeric           as order_item_discount,
    order_item_discount_rate::numeric      as order_item_discount_rate,
    order_item_product_price::numeric      as order_item_product_price,
    order_item_profit_ratio::numeric       as order_item_profit_ratio,
    order_item_quantity::int               as order_item_quantity,

    sales::numeric                         as sales,
    order_item_total::numeric              as order_item_total,
    order_profit_per_order::numeric        as order_profit_per_order,
    benefit_per_order::numeric             as benefit_per_order,

    product_card_id::int                   as product_card_id,
    product_category_id::int               as product_category_id,
    product_description,
    product_image,
    product_name,
    product_price::numeric                 as product_price,
    product_status::int                    as product_status,

    shipping_mode,
    batch_date::date                       as batch_date

from source
