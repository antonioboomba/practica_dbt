{{
    config(
        materialized='view'
    )
}}


with
src as (
    select * from {{ source('sql_server_dbo', 'order_items') }}
),

order_item as (

    select
        order_id,
        product_id,
        quantity,
        _fivetran_deleted as data_load,
        _fivetran_synced as data_synced
    from src

)

select * from order_item
