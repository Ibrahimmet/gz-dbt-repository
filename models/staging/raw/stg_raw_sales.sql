-- with 

-- source as (

--     select * from {{ source('raw', 'sales') }}

-- ),

-- renamed as (

--     select
--         date_date,
--         orders_id,
--         pdt_id as product_id,
--         revenue,
--         quantity

--     from source

-- )

-- select * from renamed

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'sales') }}
    WHERE orders_id IS NOT NULL
      AND pdt_id IS NOT NULL
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY orders_id, pdt_id ORDER BY date_date) AS row_num
    FROM source
)
SELECT *
FROM ranked
WHERE row_num = 1
