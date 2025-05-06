-- models/staging/raw/stg_raw_sales.sql

with source as (
    select 
        orders_id,
        pdt_id,
        revenue,
        quantity,
        date_date  -- Add necessary fields from raw_gz_sales
    from `grounded-access-456814-a2`.`gz_raw_data`.`raw_gz_sales`
)

select 
    orders_id,
    pdt_id,
    revenue,
    quantity,
    date_date
from source;



