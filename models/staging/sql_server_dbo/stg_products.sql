

-- Define a common table expression (CTE) named 'src' to select all columns from the 'products' table in the 'sql_server_dbo' schema.
with 
src as (
    select * from {{ source('sql_server_dbo', 'products') }}
),

-- Define otra CTE llamada 'products' con transformaciones de datos.
products as (
    -- Selecciona columnas de la CTE 'src' y aplica las funciones COALESCE y NULLIF para manejar posibles valores NULL o vacíos.
    select
        COALESCE(NULLIF(product_id, ''), 'no_product_id')::varchar(50) as product_id,
        COALESCE(NULLIF(name, ''), 'no_product_name')::varchar(50) as product_name,

        -- Aplica COALESCE para proporcionar valores predeterminados para 'price'.
        COALESCE(price::decimal(24,2), 0.00) as price_usd,

        -- Usa la función 'decode' para manejar valores NULL en la columna 'inventory' y reemplazarlos con 'empty_inventory'.
        decode(inventory,null,'empty_inventory',inventory)::int as inventory,

        -- Usa la función 'decode' para manejar valores NULL en la columna '_fivetran_deleted' y reemplazarlos con 'no_fivetran_deleted'.
        decode(_fivetran_deleted,null,'no_fivetran_deleted',_fivetran_deleted) as fivetran_del,

        -- Incluye la columna '_fivetran_synced' tal como está.
        _fivetran_synced as fivetran_synced
    from src
)

-- Selecciona todas las columnas de la CTE 'products'.
select * from products