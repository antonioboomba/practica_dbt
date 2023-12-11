with 

src_promos as (

    select * from {{ source('sql_server_dbo', 'promos') }}

),

stg_promos as (

    select
        decode(promo_id,'','no_promo_id',null,'no_promo_id',promo_id)::varchar(50) as promo_id,
        discount::float as discount_promo,
        status::varchar(50),
        decode(_fivetran_deleted,null,'no_date_load',_fivetran_deleted) as date_load,
        _fivetran_synced

    from src_promos

)

select * from stg_promos
