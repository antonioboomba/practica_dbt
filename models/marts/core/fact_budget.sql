with stg_budget as(

    select * from {{ ref('stg_budget') }}
),

dim_products as (

    select
        *
    from {{ ref('dim_products') }}
),
dim_date as (
    select
        date_key,
        date_day
    from {{ ref('dim_date') }}
),
fact_budget as(
    SELECT 
    
        a.budget_sk,
        b.product_id,
        a.target_quantity
     FROM stg_budget a 

     left join dim_products b on b.product_id = a.product_id
)

select * from fact_budget