{{ config(materialized='view') }}

WITH sales AS (
    SELECT
        date_date,
        orders_id,
        products_id,
        CAST(revenue AS FLOAT64) AS revenue,
        CAST(quantity AS INT64) AS quantity
    FROM {{ ref('stg_raw__sales') }}
),

product AS (
    SELECT
        products_id,
        CAST(purchse_price AS FLOAT64) AS purchase_price
    FROM {{ ref('stg_raw__product') }}
)

SELECT
    sales.*,
    product.purchase_price,
    (sales.quantity * product.purchase_price) AS purchase_cost,
    (sales.revenue - (sales.quantity * product.purchase_price)) AS margin
FROM sales
LEFT JOIN product
ON sales.products_id = product.products_id
