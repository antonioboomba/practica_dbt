/* 19. Ejercicios prácticos

1. ¿Cuántos usuarios tenemos?

select 
    count(distinct user_id)-1 as number_of_users -- '-1' serves to do not take into account the "no_user"
from dim_customers

2. En promedio, ¿cuánto tiempo tarda un pedido desde que se realiza hasta que se entrega?

select 
    round(avg(days_to_deliver),2) as avg_days_delivery
from fact_sales_orders_details
--RESPOSTA: 3.83 days

3. ¿Cuántos usuarios han realizado una sola compra? ¿Dos compras? ¿Tres o más compras?
        #RESP.1: 26
        #RESP.2: 28
        #RESP.3: 34


- *Nota: debe considerar una compra como un solo pedido. En otras palabras, si un usuario realiza un pedido de 3 productos, se considera que ha realizado 1 compra.*
- En promedio, ¿Cuántas sesiones únicas tenemos por hora?
*/
WITH dim_users AS (
    SELECT * 
    FROM {{ ref('dim_users') }}
),

fct_orders as (
    SELECT * FROM {{ref('fct_orders')}}
),


resp1 as (
    SELECT count(distinct user_id) as name_of_users
    FROM
    dim_users
),
resp2 as (
    SELECT * FROM fct_orders
)


select * from resp2


