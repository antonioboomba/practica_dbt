
WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_users') }}
),


dim_users AS (
     select
        {{dbt_utils.surrogate_key(['user_id'])}} as  user_Sk,
        stg_users.user_id,
        dim_addresses.address_id,
        stg_users.updated_at,
        stg_users.first_name || ' ' || stg_users.last_name as complete_name,
        stg_users.email,
        stg_users.created_at,
        stg_users.phone_number,
        stg_users.total_orders
     FROM stg_users

left join {{ref('dim_addresses')}} on stg_users.address_id = dim_addresses.address_id

)

SELECT * FROM dim_users




