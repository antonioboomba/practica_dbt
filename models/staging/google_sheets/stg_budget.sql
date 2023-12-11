with source as (
    select * from {{ source('google_sheets', 'budget')}}
),

renamed as (

    select
        _row::number as ID_ROW,
        quantity::number as Quantity,
        month::date as Month,
        product_id::VARCHAR(256) as product_id,
        _fivetran_synced::timestamp AS date_load

    from source

)

select * from renamed
