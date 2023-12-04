WITH dim_tiempo as (
    {{ dbt_date.get_date_dimension("2020-01-01","2023-01-01") }}
)

SELECT * FROM dim_tiempo