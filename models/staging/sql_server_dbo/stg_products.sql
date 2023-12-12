{{
    config(
        materialized='view'
    )
}}

-- Define a common table expression (CTE) named 'src' to select all columns from the 'products' table in the 'sql_server_dbo' schema.
with 
src as (
    select * from {{ source('sql_server_dbo', 'products') }}
),

-- Define another CTE named 'products' with data transformations.
products as (
    -- Select columns from the 'src' CTE and apply COALESCE and NULLIF functions for handling potential NULL or empty values.
    select
        COALESCE(NULLIF(product_id, ''), 'no_product_id')::varchar(50) as product_id,
        COALESCE(NULLIF(name, ''), 'no_product_name')::varchar(50) as product_name,

        -- Apply COALESCE to provide default values for 'product_id', 'name', and 'price'.
        COALESCE(product_id::varchar(50), 'default_product_id') as product_id,
        COALESCE(name::varchar(50), 'default_product_name') as product_name,
        COALESCE(price::decimal(24,2), 0.00) as price_usd,

        -- Use 'decode' function to handle NULL values in the 'inventory' column and replace them with 'empty_inventory'.
        decode(inventory,null,'empty_inventory',inventory)::int as inventory,

        -- Use 'decode' function to handle NULL values in the '_fivetran_deleted' column and replace them with 'no_fivetran_deleted'.
        decode(_fivetran_deleted,null,'no_fivetran_deleted',_fivetran_deleted) as fivetran_del,

        -- Include the '_fivetran_synced' column as is.
        _fivetran_synced as fivetran_synced
    from src
)

-- Select all columns from the 'products' CTE.
select * from products