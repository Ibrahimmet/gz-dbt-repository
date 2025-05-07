with 

source as (

    select * from {{ source('raw_gz_criteo', 'raw_gz_criteo') }}

),

renamed as (

    select

    from source

)

select * from renamed
