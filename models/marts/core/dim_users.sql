
WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_users') }}
),


renamed_casted AS (
     SELECT
        user_id,
        updated_at,
        address_id,
        last_name,
        created_at,
        phone_number,
        total_orders,
        first_name,
        email,
        _fivetran_deleted,
        _fivetran_synced
     FROM stg_users
)

SELECT * FROM renamed_casted