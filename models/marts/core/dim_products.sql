
-- Configuración de pruebas
{{ config(
    materialized='table',
    unique_key='product_id',
    tests=[
        "unique('product_id')",
        "not_null('product_name')",
        "not_null('price_usd')"
    ]
) }}


WITH stg_products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
),

stg_products_casted AS (
    SELECT
       COALESCE(product_id, 'default_id') AS product_id,
        COALESCE(price_usd, 0.00) AS price_usd,
        COALESCE(product_name, 'unknown') AS product_name,
        COALESCE(inventory, 0) AS inventory,
        COALESCE(fivetran_del, false) AS fivetran_del
    FROM stg_products
)

SELECT * FROM stg_products_casted
