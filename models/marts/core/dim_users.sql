
WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_users') }}
),


dim_users AS (
     select
        {{dbt_utils.surrogate_key(['user_id'])}} as  sk_user_key,
        stg_users.user_id,
        stg_users.updated_at,
        stg_users.first_name || ' ' || stg_users.last_name as name_full,
        stg_users.email,
        stg_users.created_at,
        stg_users.phone_number,
        stg_users.total_orders
     FROM stg_users

)

SELECT * FROM dim_users




