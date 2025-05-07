with 

source as (

    select * from {{ source('raw_gz_facebook', 'raw_gz_facebook') }}

),

renamed as (

    select

    from source

)

select * from renamed
