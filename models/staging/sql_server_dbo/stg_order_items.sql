{{
    config(
        materialized='incremental',
        unique_key = 'order_id',
        on_schema_change = 'fail'
    )
}}


with
stg_orders as (
    --Extract data  from  order_items 
    select * from {{ source('sql_server_dbo', 'order_items') }}


-- Verify it's a incremental model
    {% if is_incremental()%}
        
        -- If is incremental select the dates(five_tran_synced) > than the date in the model
        where _fivetran_synced > (select max(date_load) from {{this}})

    {% endif %}

),




order_item as (

    select
        -- Order Id
        order_id::varchar(50) as order_id,
        -- Product Id
        decode(product_id,'','no_product_id',null,'no_product_id',product_id)::varchar(50) as product_id,
        
        -- Quantity
        decode(quantity,null,'no_quantity',quantity)::int as quantity,

        -- dates
        decode(_fivetran_deleted,null,'no_fivetran_deleted',_fivetran_deleted) as data_load,
        decode(_fivetran_synced,null,'no_data_synced',_fivetran_synced) as data_synced

    from stg_orders

)

select * from order_item
