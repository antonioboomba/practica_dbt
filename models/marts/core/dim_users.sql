with 

source as (

    select * from {{ ref('stg_users') }}

),

renamed as (

    select
        user_id
        address_id,
        last_name,
        phone_number,
        total_orders,
        created_at,
        updated_at,
        first_name,
        email,
        _fivetran_deleted,
        _fivetran_synced
        date_load,
    from source

)

select * from renamed
