

WITH stg_orders AS (SELECT * FROM {{ ref('stg_orders') }})
SELECT
    -- Detalles del producto
    COALESCE(stg_orders.order_id, 'N/A') AS order_id,
    COALESCE(stg_orders.shipping_service, 'N/A') AS shipping_service,
    COALESCE(stg_orders.shipping_cost_usd, 0.0) AS shipping_cost_usd,
    COALESCE(stg_orders.address_id, 'N/A') AS address_id,
    COALESCE(stg_orders.created_at_utc, '1970-01-01'::timestamp) AS created_at_utc,
    COALESCE(stg_orders.promo_id, 'N/A') AS promo_id,
    COALESCE(stg_orders.estimated_delivery_at_utc, '1970-01-01'::timestamp) AS estimated_delivery_at_utc,
    COALESCE(stg_orders.order_cost_usd, 0.0) AS order_cost_usd,
    COALESCE(stg_orders.user_id, 'N/A') AS user_id,
    COALESCE(stg_orders.order_total_usd, 0.0) AS order_total_usd,
    COALESCE(stg_orders.delivered_at_utc, '1970-01-01'::timestamp) AS delivered_at_utc,
    COALESCE(stg_orders.tracking_id, 'N/A') AS tracking_id,
    COALESCE(stg_orders.status, 'N/A') AS status,
    COALESCE(stg_orders._fivetran_synced, 'N/A') AS _fivetran_synced
FROM stg_orders

left join {{ref('stg_addresses')}} on stg_orders.address_id = stg_addresses.address_id
left join {{ref('stg_promos')}} on stg_orders.promo_id = stg_promos.promo_id

