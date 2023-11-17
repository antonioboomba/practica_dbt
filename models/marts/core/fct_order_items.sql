{{
    config(
        materialized='table'
    )
}}
WITH src_order_items AS (
    SELECT * 
    FROM {{ ref('stg_order_items') }}
),


stg_orders_items_casted AS (
    SELECT
        order_id,
        product_id,
        quantity,
        _fivetran_deleted,
        _fivetran_synced
    FROM src_order_items
    )

SELECT * FROM stg_orders_items_casted