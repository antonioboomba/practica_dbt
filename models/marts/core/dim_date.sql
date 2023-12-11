-- models/my_project/dim_date.sql

WITH date_dimension as (
    select * from {{ source('sql_server_dbo', 'stg_date') }}
),

SELECT * FROM date_dimension
