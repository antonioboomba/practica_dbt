-- Configuración del modelo
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='fail'
) }}

-- Definición de CTE para stg_orders
WITH stg_orders AS (
    SELECT *
    FROM {{ source('sql_server_dbo', 'order_items') }}
),

-- Definición de CTE para max_synced_date
max_synced_date AS (
    SELECT MAX(_fivetran_synced) AS max_synced
    FROM stg_orders
),

-- Definición de CTE para order_item
order_item AS (
    SELECT
        -- Order Id
        order_id::varchar(50) AS order_id,
        -- Product Id
        COALESCE(NULLIF(product_id, ''), 'no_product_id')::varchar(50) AS product_id,
        -- Quantity
        COALESCE(NULLIF(quantity, ''), 'no_quantity')::int AS quantity,
        -- Fecha de carga
        _fivetran_synced AS date_load
    FROM stg_orders
    -- Filtra solo las filas que han sido actualizadas desde la última ejecución
    WHERE _fivetran_synced > (SELECT max_synced FROM max_synced_date)
)

-- Selecciona todos los datos de order_item
SELECT * FROM order_item