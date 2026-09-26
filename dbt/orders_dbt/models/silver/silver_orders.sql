{{ config(
    materialized='table'
) }}

with ranked_orders as (

    select
        order_id,
        customer_id,
        amount,
        status,
        updated_at,

        row_number() over (
            partition by order_id
            order by updated_at desc
        ) as rn

    from {{ ref('stg_orders') }}

)

select
    order_id,
    customer_id,
    amount,
    status,
    updated_at
from ranked_orders
where rn = 1