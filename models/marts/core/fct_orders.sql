{{
  config(
    materialized='table'
  )
}}


WITH stg_orders AS (SELECT * FROM {{ ref('stg_orders') }})
SELECT
    stg_orders.order_id,
    stg_orders.shipping_service,
    stg_orders.shipping_cost,
    stg_orders.address_id,
    stg_orders.created_at,
    stg_orders.promo_id,
    stg_orders.estimated_delivery_at,
    cstg_orders.order_cost,
    stg_orders.user_id,
    stg_orders.order_total,
    stg_orders.delivered_at,
    stg_orders.tracking_id,
    stg_orders.status,
    stg_orders._fivetran_deleted,
    stg_orders._fivetran_synced  
FROM stg_order

left join {{ref('stg_addresses')}} on stg_orders.address_id = stg_addresses.address_id
left join {{ref('stg_promos')}} on stg_orders.promo_id = stg_promos.promo_id

