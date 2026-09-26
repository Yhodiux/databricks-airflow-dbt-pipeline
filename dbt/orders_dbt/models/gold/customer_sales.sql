{{ config(
    materialized='table'
) }}

select
    customer_id,
    count(order_id) as total_orders,
    sum(amount) as total_amount
from {{ ref('silver_orders') }}
where status = 'COMPLETED'
group by customer_id