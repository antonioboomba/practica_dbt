with stg_addresses as (
    select * from {{ref('stg_addresses')}} 
),


dim_addresses as(
    select
        {{dbt_utils.surrogate_key(['stg_addresses.address_id'])}} as address_sk,
        address_id,
        country,
        state,
        address,
        numeric_zipcode
    from stg_addresses
)
select * from dim_addresses