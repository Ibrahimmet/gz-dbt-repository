

with base as (
    select * from {{ ref('int_sales_margin') }}  
)

select
    orders_id,
    MIN(date_date) as date_date,  
    SUM(quantity) as quantity,
    SUM(revenue) as revenue,
    SUM(purchase_cost) as purchase_cost,
    SUM(margin) as margin
from base
group by orders_id;
