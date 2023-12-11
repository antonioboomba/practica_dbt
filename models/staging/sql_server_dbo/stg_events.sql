{{
    config(
        materialized='view'
    )
}}

WITH src_events AS (
    SELECT * 
    FROM {{ source ('sql_server_dbo', 'events') }}
    ),

stg_events AS (
    SELECT

        --asign sk_event_id
        {{dbt_utils.surrogate_key(["event_id"])}} as sk_event_id,

        --events information
         decode(event_id,'','no_event_id',null,'no_event_id',event_id)::varchar(50) as event_id
        ,decode(page_url,'','no_page_url',null,'no_page_url',page_url)::varchar(100) as page_url
        ,decode(event_type,'','no_event_type',null,'no_event_type',event_type)::varchar(60) as event_type
        
        -- user information        
        ,decode(user_id,'','no_user_id',null,'no_user_id',user_id)::varchar(50) as user_id
        
        
        --product information
        ,decode(product_id,'','no_product_id',null,'no_product_id',product_id)::varchar(200) as product_id
        ,decode(session_id,'','no_session_id',null,'no_session_id',session_id)::varchar(50) as session_id
        
        --dates
        ,created_at::timestamp as created_at_utc
        ,decode(order_id,'','no_order_id',null,'no_order_id',order_id)::varchar(100) as order_id
        ,decode(_fivetran_deleted,'','no_synced',null,'no_synced',_fivetran_deleted) as fivetran_del
        ,_fivetran_synced
    FROM src_events
    )

SELECT * FROM stg_events