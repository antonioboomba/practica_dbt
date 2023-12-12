WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
),

-- Definición de CTE para stg_events
stg_events AS (
    SELECT
        -- Asigna una clave surrogate para sk_event_id utilizando dbt_utils.surrogate_key
        {{dbt_utils.surrogate_key(["event_id"])}} AS sk_event_id,

        -- Información sobre eventos con transformación para manejar valores nulos o vacíos
        COALESCE(NULLIF(event_id, ''), 'no_event_id')::VARCHAR(50) AS event_id,
        COALESCE(NULLIF(page_url, ''), 'no_page_url')::VARCHAR(100) AS page_url,
        COALESCE(NULLIF(event_type, ''), 'no_event_type')::VARCHAR(60) AS event_type,

        -- Información sobre el usuario        
        COALESCE(DECODE(user_id, '', 'no_user_id', user_id), 'no_user_id')::VARCHAR(50) AS user_id,

        -- Información sobre los productos y session_id con transformación para manejar valores nulos o vacíos
        COALESCE(NULLIF(product_id, ''), 'no_product_id')::VARCHAR(200) AS product_id,
        COALESCE(NULLIF(session_id, ''), 'no_session_id')::VARCHAR(50) AS session_id,

        -- Transformación de fechas
        COALESCE(CAST(created_at AS TIMESTAMP), '0000-00-00 00:00:00') AS _fivetran_deleted,
        COALESCE(NULLIF(order_id, ''), 'no_order_id')::VARCHAR(100) AS order_id,
        COALESCE(CAST(created_at AS TIMESTAMP), '0000-00-00 00:00:00') AS date_load
    FROM src_events
)

-- Selecciona todos los datos de la tabla de etapa stg_events
SELECT * FROM stg_events