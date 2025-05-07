{{ config(materialized='view') }}

SELECT
    orders_id,
    CAST(shipping_fee AS FLOAT64) AS shipping_fee,
    CAST(shipping_fee_1 AS FLOAT64) AS shipping_fee_1,
    CAST(logCost AS FLOAT64) AS log_cost,
    CAST(ship_cost AS FLOAT64) AS ship_cost
FROM {{ source('raw', 'raw_gz_ship') }}

