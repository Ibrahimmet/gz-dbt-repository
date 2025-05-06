-- models/staging/stg_raw_sales.sql

with raw_sales as (
    select 
        orders_id,
        pdt_id,
        revenue,
        quantity,
        date_date
    from 
        {{ source('raw', 'raw_gz_sales') }}  -- Reference to raw_gz_sales source
)

select * from raw_sales;

-- models/staging/stg_raw_product.sql

with raw_product as (
    select 
        products_id,
        name,
        category,
        purchSE_PRICE
    from 
        {{ source('raw', 'raw_gz_product') }}  -- Reference to raw_gz_product source
)

select * from raw_product;

-- models/staging/stg_raw_ship.sql

with raw_ship as (
    select 
        orders_id,
        shipping_fee,
        shipping_fee_1,
        logCost,
        ship_cost
    from 
        {{ source('raw', 'raw_gz_ship') }}  -- Reference to raw_gz_ship source
)

select * from raw_ship;

-- models/intermediate/int_sales_margin.sql

with sales as (
    select 
        orders_id,
        pdt_id,
        revenue,
        quantity
    from 
        {{ ref('stg_raw_sales') }}  -- Reference to stg_raw_sales model
),
product as (
    select 
        products_id,
        purchSE_PRICE
    from 
        {{ ref('stg_raw_product') }}  -- Reference to stg_raw_product model
),
shipping as (
    select 
        orders_id,
        shipping_fee,
        logCost
    from 
        {{ ref('stg_raw_ship') }}  -- Reference to stg_raw_ship model
)

select 
    s.orders_id,
    s.pdt_id,
    s.revenue,
    s.quantity,
    p.purchSE_PRICE,
    s.revenue - (p.purchSE_PRICE * s.quantity) as purchase_cost,
    s.revenue - (p.purchSE_PRICE * s.quantity) - sh.logCost as margin
from sales s
join product p on s.pdt_id = p.products_id
left join shipping sh on s.orders_id = sh.orders_id;

version: 2

sources:
  - name: raw
    schema: gz_raw_data
    tables:
      - name: raw_gz_sales
        description: Raw sales data
        columns:
          - name: orders_id
            description: Unique identifier for each customer order.
          - name: pdt_id
            description: Product ID associated with each order.
          - name: revenue
            description: Revenue from the sale.
          - name: quantity
            description: Quantity sold.

      - name: raw_gz_product
        description: Raw product data
        columns:
          - name: products_id
            description: Unique identifier for each product.
          - name: name
            description: Product name.
          - name: category
            description: Product category.
          - name: purchSE_PRICE
            description: Purchase price.

      - name: raw_gz_ship
        description: Raw shipping data
        columns:
          - name: orders_id
            description: Unique identifier for each order.
          - name: shipping_fee
            description: Shipping fee charged to the customer.
          - name: shipping_fee_1
            description: Duplicate field, to be validated.
          - name: logCost
            description: Internal logistics cost.
          - name: ship_cost
            description: Actual cost of shipping.
