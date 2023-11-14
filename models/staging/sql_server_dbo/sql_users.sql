
{{
    config(
        materialized='view'
    )


}}
with 
src as (
    select * from {{ source('sql_server_dbo', 'users') }}
),

users as (

    select
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

    from src

)

select * from users


