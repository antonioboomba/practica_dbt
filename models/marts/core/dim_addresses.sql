

-- Definición de una tabla temporal o vista llamada stg_addresses,
-- que contiene todos los datos de la fuente 'stg_addresses'
with stg_addresses as (
    select * from {{ref('stg_addresses')}} 
),

-- Definición de la dimensión dim_addresses,
-- que incluye la generación de una clave surrogate utilizando dbt_utils.surrogate_key,
-- y selecciona algunas columnas específicas de la tabla stg_addresses

dim_addresses as (
    select
        {{dbt_utils.surrogate_key(['stg_addresses.address_id'])}} as address_sk,
          address_id,
        -- Utiliza COALESCE para manejar columnas vacías
        COALESCE(country, 'unknown') AS country,
        COALESCE(state, 'unknown') AS state,
        COALESCE(address, 'unknown') AS address,
        COALESCE(numeric_zipcode, 0) AS numeric_zipcode
    from stg_addresses
)

-- Selecciona todos los datos de la dimensión dim_addresses
select * from dim_addresses


-- Configuración de pruebas para verificar las condiciones deseadas
