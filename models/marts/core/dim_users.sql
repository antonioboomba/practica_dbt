
WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_users') }}
),

dim_users AS (
    SELECT
        dim_date.date_key,
        {{ dbt_utils.surrogate_key(['stg_users.user_id']) }} AS sk_user_key,
        stg_users.user_id,
        dim_addresses.address_sk,
        stg_users.first_name || ' ' || stg_users.last_name AS name_full,
        stg_users.email,
        stg_users.created_at_utc,
        stg_users.updated_at_utc,
        stg_users.phone_number
        
    FROM stg_users
    LEFT JOIN {{ ref('dim_addresses') }} ON stg_users.address_id = dim_addresses.address_id
    LEFT JOIN {{ ref('dim_date') }} ON stg_users.created_at_utc = dim_date.date_day 
)

SELECT * FROM dim_users




