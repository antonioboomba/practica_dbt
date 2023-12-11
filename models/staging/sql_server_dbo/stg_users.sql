with 

source as (
    select * from {{ source('sql_server_dbo', 'users') }}

),

renamed as (
    select
        user_id,
        decode(address_id,'','no_address_id',null,'no_address_id',address_id)::varchar(50) as address_id,
        decode(last_name,'','no_last_name',null,'no_last_name',last_name)::varchar(50) as last_name,
        decode(phone_number,'','no_phone',null,'no_phone',phone_number) as phone,
        decode(first_name,'','no_first_name',null,'no_first_name',first_name)::varchar(50) as first_name,
        decode(email,'','no_email',null,'no_email',email)::varchar(60) as email,
        cast(updated_at as date) as updated_at_utc,
        coalesce(_fivetran_deleted, false)::boolean as row_deleted,
        
    from source
)

select * from renamed
