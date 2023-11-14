
{{
    config(
        materialized='view'
    )
}}
with 

src as (

    select * from {{ source('sql_server_dbo', 'addresses') }}

),

addresses as (

    select
        address_id,
        zipcode,
        country,
        address,
        state,
        _fivetran_deleted,
        _fivetran_synced

    from src

)

select * from addresses
