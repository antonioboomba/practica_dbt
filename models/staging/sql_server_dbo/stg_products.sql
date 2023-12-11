
{{
    config(
        materialized='view'
    )
}}

with 
src as (
    select * from {{ source('sql_server_dbo', 'products') }}
),

products as (
    select
        product_id::varchar(50) as product_id,
        name::varchar(50) as product_name,
        price::decimal(24,2) as price_usd,
        decode(inventory,null,'empty_inventory',inventory)::int as inventory,
        decode(_fivetran_deleted,null,'no_fivetran_deleted',_fivetran_deleted) as fivetran_del,
        _fivetran_synced as fivetran_synced
    from src
)

select * from products