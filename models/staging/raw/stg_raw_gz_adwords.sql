-- models/staging/raw/stg_raw_gz_adwords.sql
with adwords_raw as (
    select * from {{ source('raw_gz_adwords', 'raw_gz_adwords') }}
)

select
    camPGN_name as campaign_name,        -- Renaming campaign name
    cast(ads_cost as float64) as ads_cost  -- Casting ads_cost to float64
from adwords_raw
