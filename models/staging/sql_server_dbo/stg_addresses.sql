with addresses as (

    select * 
    from {{ source('sql_server_dbo', 'addresses') }}
),

stg_addresses as (
    select

        --address id
        decode(address_id,'','no_address_id',null,'no_address_id',address_id)::varchar(50) as address_id,

    --address information
        decode(country,'','no_country',null,'no_country',country)::varchar(50) as country,
        decode(state,'','no_state',null,'no_state',state)::varchar(50) as state,
        CAST(zipcode AS INT) AS numeric_zipcode, -- Cast as int the zipcode
        decode(address,'','no_address',null,'no_address',address)::varchar(50) as address
    from addresses
)

select * from stg_addresses
