
-- Definición de la tabla "stg_orders" que representa los pedidos
with orders as (
    select *
    from {{ source('sql_server_dbo', 'orders') }}
    
    -- Filtra datos incrementales basados en la columna "_fivetran_synced"
    {% if is_incremental() %}
        where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
    {% endif %}
),

-- Transformaciones en los datos de pedidos para la tabla "stg_orders"
stg_orders as (
    select
        -- Transformaciones en las columnas de identificación
        COALESCE(NULLIF(order_id, ''), 'no_order_id')::varchar(50) as order_id,
        COALESCE(NULLIF(user_id, ''), 'no_user_id')::varchar(50) as user_id,
        COALESCE(NULLIF(address_id, ''), 'no_address_id')::varchar(50) as address_id,
        COALESCE(NULLIF(promo_id, ''), 'no_promo')::varchar(50) as promo_id,
       
        -- Transformaciones en las columnas relacionadas con la entrega/envío
        COALESCE(NULLIF(status, ''), 'no_status')::varchar(50) as status,
        COALESCE(NULLIF(tracking_id, ''), 'no_tracking_id')::varchar(50) as tracking_id,
        COALESCE(NULLIF(shipping_service, ''), 'no_shipping_service')::varchar(50) as shipping_service,
      
        -- Transformaciones en las columnas de fechas
        created_at::timestamp as created_at_utc,
        estimated_delivery_at::timestamp as estimated_delivery_at_utc,
        delivered_at::timestamp as delivered_at_utc,

        -- Medidas transformadas
        COALESCE(order_cost, 0.00)::decimal(24,2) as order_cost_usd,
        COALESCE(shipping_cost, 0.00)::decimal(24,2) as shipping_cost_usd,
        COALESCE(order_total, 0.00)::decimal(24,2) as order_total_usd,
        _fivetran_synced::timestamp as _fivetran_synced
    from orders
)

-- Selecciona todos los campos de la tabla "stg_orders"
select * from stg_orders