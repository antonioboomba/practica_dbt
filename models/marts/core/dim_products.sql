{{
    config(
        materialized='table'
    )
}}

WITH stg_promos AS (
    SELECT * 
    FROM {{ ref('stg_products') }}
),

stg_promos_casted as 
(
    SELECT 
        product_id,
        price,
        name,
        inventory,
        _fivetran_deleted,
        _fivetran_synced
    FROM stg_promos
)

SELECT * FROM stg_promos_casted