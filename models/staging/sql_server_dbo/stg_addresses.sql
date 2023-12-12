-- Definición de una CTE llamada 'addresses' para seleccionar todas las columnas de la tabla 'addresses' en el esquema 'sql_server_dbo'.
with addresses as (
    select * 
    from {{ source('sql_server_dbo', 'addresses') }}
),

-- Definición de otra CTE llamada 'stg_addresses' con transformaciones de datos.
stg_addresses as (
    select
        -- Dirección ID: Utiliza 'decode' para manejar valores específicos en 'address_id' y realiza un casting a varchar(50).
        COALESCE(NULLIF(address_id, ''), 'no_address_id')::varchar(50) as address_id,
        -- Información de la dirección:
        COALESCE(NULLIF(country, ''), 'no_country')::varchar(50) as country, -- Manejo de valores nulos o vacíos en 'country'.
        -- Manejo de valores nulos o vacíos en 'state'.
        COALESCE(NULLIF(state, ''), 'no_state')::varchar(50) as state, 
        -- Conversión del tipo de dato de 'zipcode' a INT.
        CAST(zipcode AS INT) AS numeric_zipcode, 
        -- Manejo de valores nulos o vacíos en 'address'.
        COALESCE(NULLIF(address, ''), 'no_address')::varchar(50) as address 
    from addresses
)

-- Selecciona todas las columnas de la CTE 'stg_addresses'.
select * from stg_addresses