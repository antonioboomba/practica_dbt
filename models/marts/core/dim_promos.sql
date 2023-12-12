-- Configuración del modelo
{{ config(
    materialized='table',
    unique_key='promo_id',
    tests=[
        "unique('promo_id')",
        "not_null('status')",
        "not_null('date_load')"
    ]
) }}

with stg_promos as (
    select * from {{ ref('stg_promos') }}
),


dim_promos as(
    select
       {{ dbt_utils.surrogate_key(['stg_promos.promo_id']) }} AS promo_sk,
        promo_id,
        COALESCE(CAST(discount_promo AS float), 0.0) AS discount_promo, -- Asegura que discount_promo sea de tipo float y maneja valores nulos
        COALESCE(status, 'unknown') AS status, -- Asegura que status no sea nulo y asigna un valor predeterminado si lo es
         date_load, -- Asegura que date_load no sea nulo y asigna un valor predeterminado si lo es
        _fivetran_synced

    from stg_promos
    )

select * from dim_promos
