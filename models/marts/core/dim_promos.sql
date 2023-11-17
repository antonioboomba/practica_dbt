{{
    config(
        materialized='table'
    )
}}

WITH stg_promos AS (
    SELECT * 
    FROM {{ ref('stg_promos') }}
),



stg_promos_casted as 
(
    SELECT 
        promo_id,
        discount,
        status,
        _fivetran_deleted,
        _fivetran_synced
    FROM stg_promos
)

SELECT * FROM stg_promos_casted