-- Configuración del modelo
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='fail'
) }}

-- Documentación del modelo

-- CTE para cargar datos de order_items
WITH stg_orders AS (
    SELECT *
    FROM {{ source('sql_server_dbo', 'order_items') }}
    
    -- Verifica si es un modelo incremental
    {% if is_incremental() %}
        -- Si es incremental, selecciona las fechas (_fivetran_synced) mayores que la fecha en el modelo
        WHERE _fivetran_synced > (SELECT MAX(date_load) FROM {{ this }})
    {% endif %}
),

-- Transformaciones y asignación de valores predeterminados
order_item AS (
    SELECT
        -- Order Id
        COALESCE(order_id::varchar(50), 'no_order_id') AS order_id,
        -- Product Id
        COALESCE(NULLIF(product_id, ''), 'no_product_id') AS product_id,
        -- Quantity
        COALESCE(CAST(quantity AS int), 0) AS quantity,
        -- Fechas
        COALESCE(_fivetran_synced, 'no_data_synced') AS date_load
    FROM stg_orders
)

-- Selección final
SELECT * FROM order_item