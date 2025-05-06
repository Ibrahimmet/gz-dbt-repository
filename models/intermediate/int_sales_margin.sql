-- -- -- models/intermediate/int_sales_margin.sql
-- -- with
-- --     sales as (
-- --         select orders_id, pdt_id, revenue, quantity from {{ ref("stg_raw_sales") }}  -- Reference to stg_raw_sales model
-- --     ),
-- --     product as (
-- --         select products_id, purchse_price from {{ ref("stg_raw_product") }}  -- Reference to stg_raw_product model
-- --     ),
-- --     shipping as (
-- --         select orders_id, shipping_fee, logcost from {{ ref("stg_raw_ship") }}  -- Reference to stg_raw_ship model
-- --     )

-- -- select
-- --     s.orders_id,
-- --     s.pdt_id,
-- --     s.revenue,
-- --     s.quantity,
-- --     p.purchse_price,
-- --     -- Calculate purchase cost (purchSE_PRICE * quantity)
-- --     (p.purchse_price * s.quantity) as purchase_cost,
-- --     -- Calculate margin (revenue - purchase cost - shipping cost)
-- --     (s.revenue - (p.purchse_price * s.quantity) - sh.logcost) as margin
-- -- from sales s
-- -- join product p on s.pdt_id = p.products_id  -- Join on product_id
-- -- left join shipping sh on s.orders_id = sh.orders_id  -- Left join on orders_id for shipping data
-- -- ;

-- -- models/intermediate/int_sales_margin.sql

-- -- models/intermediate/int_sales_margin.sql

-- with sales as (
--     select 
--         orders_id,
--         pdt_id,
--         revenue,
--         quantity
--     from {{ ref('stg_raw_sales') }}
-- ),

-- product as (
--     select 
--         products_id,
--         purchase_price
--     from {{ ref('stg_raw_product') }}
-- ),

-- shipping as (
--     select 
--         orders_id,
--         shipping_fee,
--         logCost,
--         ship_cost
--     from {{ ref('stg_raw_ship') }}
-- )

-- select 
--     s.orders_id,
--     s.pdt_id,
--     s.revenue,
--     s.quantity,
--     p.purchase_price,
--     sh.logCost as shipping_cost,

--     -- Calculate purchase cost
--     s.quantity * p.purchase_price as purchase_cost,

--     -- Calculate margin
--     s.revenue - (s.quantity * p.purchase_price) - sh.logCost as margin

-- from sales s
-- join product p on s.pdt_id = p.products_id
-- left join shipping sh on s.orders_id = sh.orders_id



with sales as (
    select 
        orders_id,
        pdt_id,
        revenue,
        quantity
    from {{ ref('stg_raw_sales') }}  
),

product as (
    select 
        products_id,
        purchase_price
    from {{ ref('stg_raw_product') }}  
),

shipping as (
    select 
        orders_id,
        shipping_fee,
        logCost,
        ship_cost
    from {{ ref('stg_raw_ship') }}  
)

select 
    s.orders_id,
    s.pdt_id,
    s.revenue,
    s.quantity,
    p.purchase_price,
    sh.logCost as shipping_cost,

    
    s.quantity * p.purchase_price as purchase_cost,

    
    s.revenue - (s.quantity * p.purchase_price) - sh.logCost as margin

from sales s
join product p on s.pdt_id = p.products_id  
left join shipping sh on s.orders_id = sh.orders_id;  
