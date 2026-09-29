select
    o.order_item_id,
    o.order_id,
    o.order_customer_id,
    o.customer_id,

    o.order_date,
    o.shipping_date,
    o.days_for_shipping_real,
    o.days_for_shipment_scheduled,
    o.days_for_shipping_real - o.days_for_shipment_scheduled as shipping_delay_days,

    o.delivery_status,
    o.late_delivery_risk,

    sm.shipping_mode_id,

    o.market,
    o.order_region,
    o.order_country,
    o.order_state,
    o.order_city,

    o.category_id,
    o.category_name,
    o.product_card_id,
    o.product_name,
    o.product_price,

    o.order_item_quantity,
    o.sales,
    o.order_item_total,
    o.order_item_discount,
    o.order_item_discount_rate,
    o.order_profit_per_order,
    o.order_item_profit_ratio,
    o.benefit_per_order,

    o.batch_date

from {{ ref('stg_orders') }} o
left join {{ ref('dim_shipping_mode') }} sm
    on o.shipping_mode = sm.shipping_mode
