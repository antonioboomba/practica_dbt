


-- Definición de una tabla temporal o vista llamada src_events,
-- que contiene todos los datos de la fuente 'events' en el esquema 'sql_server_dbo'

WITH src_events AS (
    SELECT * 
    FROM {{ source ('sql_server_dbo', 'events') }}
    ),

-- Definición de la tabla de etapa stg_events,
-- que incluye transformaciones y asignaciones de claves

stg_events AS (
    SELECT

        -- Asigna una clave surrogate para sk_event_id utilizando dbt_utils.surrogate_key
        {{dbt_utils.surrogate_key(["event_id"])}} as sk_event_id,

        -- Información sobre eventos con transformación para manejar valores nulos o vacíos
        COALESCE(NULLIF(event_id,''), 'no_event_id')::varchar(50) as event_id,
        COALESCE(NULLIF(page_url,''), 'no_page_url')::varchar(100) as page_url,
        COALESCE(NULLIF(event_type,''), 'no_event_type')::varchar(60) as event_type,

        --Información sobre el usuario        
        decode(user_id,'','no_user_id',null,'no_user_id',user_id)::varchar(50) as user_id,

     -- Información sobre los productos y session_id con transformación para manejar valores nulos o vacíos
        COALESCE(NULLIF(product_id,''), 'no_product_id')::varchar(200) as product_id,
        COALESCE(NULLIF(session_id,''), 'no_session_id')::varchar(50) as session_id,

        
        -- Transformación de fechas
        COALESCE(CAST(created_at AS timestamp), '0000-00-00 00:00:00') AS _fivetran_deleted,
        COALESCE(NULLIF(order_id,''), 'no_order_id')::varchar(100) as order_id,
        COALESCE(CAST(_fivetran_deleted AS boolean), false) AS _fivetran_deleted,
        COALESCE(CAST(created_at AS timestamp), '0000-00-00 00:00:00') AS date_load


    FROM src_events
    )

-- Selecciona todos los datos de la tabla de etapa stg_events

SELECT * FROM stg_events