-- Este código define una expresión común de tabla (CTE) llamada 'users' para seleccionar todas las columnas de la tabla 'users' en el esquema 'sql_server_dbo'.
WITH users AS (
    SELECT *
    FROM {{ source('sql_server_dbo', 'users') }}
),

-- Luego, se define otra CTE llamada 'stg_users' con transformaciones de datos.
stg_users AS (
    -- En esta sección, se seleccionan columnas específicas de la CTE 'users' y se aplican diversas transformaciones.
    SELECT
        user_id::varchar(50) AS user_id, -- La columna 'user_id' se convierte a varchar(50).
        address_id::varchar(50) AS address_id,
        first_name::varchar(50) AS first_name,
        last_name::varchar(50) AS last_name,
        email::varchar(50) AS email,
        phone_number::varchar(50) AS phone_number,
        CAST(created_at AS date) AS created_at_utc, -- La columna 'created_at' se convierte al tipo de dato 'date'.
        CAST(updated_at AS date) AS updated_at_utc, -- La columna 'updated_at' se convierte al tipo de dato 'date'.
        COALESCE(_fivetran_deleted, false)::boolean AS row_deleted, -- La columna '_fivetran_deleted' se maneja con COALESCE para reemplazar valores NULL con 'false' y se realiza un cast a tipo de dato booleano.
        _fivetran_synced AS date_load
    FROM users
)

-- Finalmente, se seleccionan todas las columnas de la CTE 'stg_users'.
SELECT * FROM stg_users