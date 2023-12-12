-- Genera dinámicamente una dimensión de fechas llamada stg_date
with stg_date as (
    {{ dbt_date.get_date_dimension("2018-12-31", "2025-12-31") }}
)

-- Selecciona todos los registros de la dimensión stg_date
select * from stg_date