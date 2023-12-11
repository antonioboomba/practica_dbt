{{
  config(
    materialized='table'
  )
}}

WITH stg_events AS (
    SELECT * 
    FROM {{ ref('stg_events') }}
),
dim_users as (
    select * from {{ref('dim_users')}}
),
dim_products as ( 
    select * from {{ref('dim_products')}}
),


fact_events as(
    select
        a.event_id,
        a.session_id,
        b.user_sk,
        c.event_types_sk,
        a.event_type,
        d.product_sk,
        e.order_sk,
        f.date_key,
        a.created_at_utc,
        a.page_url

    from stg_events a 
    left join dim_users b on b.user_id = a.user_id
    left join dim_event_types c on c.event_type= a.event_type
    left join dim_products d on d.product_id = a.product_id
    left join dim_sales_orders e on e.order_id = a.order_id
    left join dim_date f on f.date_day = a.created_at_utc_date
    
    order by 2,9
)

SELECT * FROM fact_events
