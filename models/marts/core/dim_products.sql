{{
    config(
        materialized='table'
    )
}}

WITH stg_products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
),

stg_products_casted AS (
    SELECT
        product_id,
        price,
        name,
        inventory,
        _fivetran_deleted,
        _fivetran_synced
    FROM stg_products
)

SELECT * FROM stg_products_casted
