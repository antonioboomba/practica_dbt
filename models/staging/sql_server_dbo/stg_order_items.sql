{{
    config(
        materialized='incremental',
        unique_key = 'order_id',
        on_schema_change = 'fail'
    )
}}


WITH stg_orders as (
    SELECT *
    FROM {{ source('sql_server_dbo', 'order_items') }}
),

max_synced_date AS (
    SELECT MAX(_fivetran_synced) AS max_synced
    FROM stg_orders
),



order_item as (

    select
        -- Order Id
        order_id::varchar(50) as order_id,
        -- Product Id
        decode(product_id,'','no_product_id',null,'no_product_id',product_id)::varchar(50) as product_id,
        -- Quantity
        decode(quantity,null,'no_quantity',quantity)::int as quantity,
        -- dates
        decode(_fivetran_synced,null,'no_data_synced',_fivetran_synced) as date_load

    from stg_orders

)

select * from order_item
