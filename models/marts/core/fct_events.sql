{{
  config(
    materialized='table'
  )
}}

WITH stg_events AS (
    SELECT * 
    FROM {{ ref('stg_events') }}
),


stg_events_casted AS (
     SELECT
        event_id
        ,page_url
        ,event_type
        ,user_id
        ,product_id
        ,session_id
        ,created_at
        ,order_id
        ,_fivetran_deleted
        ,_fivetran_synced
    FROM stg_events
    )

SELECT * FROM stg_events_casted
