with s_nytimes_articles as (

    select * from {{ ref('stg__nytimes_articles') }}

) 

select * from s_nytimes_articles
