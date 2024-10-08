with source as (

    select * from {{ source('prototypes', 'conversation_briefs') }}

)

select * from source
