-- Se define una expresión común de tabla (CTE) llamada 'src_promos' para seleccionar todas las columnas de la tabla 'promos' en el esquema 'sql_server_dbo'.
with 
src_promos as (
    select * from {{ source('sql_server_dbo', 'promos') }}
),

-- Se define otra CTE llamada 'stg_promos' con transformaciones de datos.
stg_promos as (
    -- Selecciona columnas de la CTE 'src_promos' y utiliza la función 'decode' para manejar valores específicos en la columna 'promo_id'.
    select
        decode(promo_id, '', 'no_promo_id', null, 'no_promo_id', promo_id)::varchar(50) as promo_id,
        -- Convierte la columna 'discount' a tipo de dato float.
        discount::float as discount_promo, 
        status::varchar(50) as status,
        -- Utiliza la función 'decode' para manejar valores NULL en la columna '_fivetran_deleted' y reemplazarlos con 'no_date_load'.
        decode(_fivetran_deleted, null, 'no_date_load', _fivetran_deleted) as date_load,
        _fivetran_synced

    from src_promos
)

-- Selecciona todas las columnas de la CTE 'stg_promos'.
select * from stg_promos