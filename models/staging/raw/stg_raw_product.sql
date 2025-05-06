-- models/staging/raw/stg_raw_product.sql

with source as (
    select 
        products_id,
        cast(purchse_price as FLOAT64) as purchase_price  -- Fix typo if needed (e.g., 'purchse_price' instead of 'purchase_price')
    from `grounded-access-456814-a2`.`gz_raw_data`.`raw_gz_product`
)

select 
    products_id,
    purchase_price
from source;
