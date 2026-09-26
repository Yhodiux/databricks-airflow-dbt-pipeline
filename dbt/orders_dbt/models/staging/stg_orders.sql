select
    order_id,
    customer_id,
    cast(amount as decimal(10,2)) as amount,
    status,
    cast(updated_at as timestamp) as updated_at
from {{ source('bronze', 'orders') }}