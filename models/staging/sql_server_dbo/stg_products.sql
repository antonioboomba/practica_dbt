
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
        product_id,
        price,
        name,
        inventory,
        _fivetran_deleted,
        _fivetran_synced

    from src
)

select * from products
