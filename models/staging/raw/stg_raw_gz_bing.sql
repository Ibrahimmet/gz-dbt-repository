with 

source as (

    select * from {{ source('raw_gz_bing', 'raw_gz_bing') }}

),

renamed as (

    select

    from source

)

select * from renamed
