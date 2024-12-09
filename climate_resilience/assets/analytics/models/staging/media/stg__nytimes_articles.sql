{% set partition_def = 'partition by article_url, media_source order by article_publication_ts RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING' %}

with source as (

    select * from {{ source('media', 'nytimes_articles') }}

),

base as (

    select distinct
        cast(link as string) as article_url,
        'nytimes' as media_source,

        cast(title as string) as article_title,
        cast(summary as string) as article_summary,
        cast(author as string) as article_author,
        lower(cast(tags as string)) as article_tags,
        cast(medias as string) as article_medias,

        cast(published_ts as timestamp) as article_publication_ts
    
    from source

),

dedup as (
    
    select distinct
        article_url,
        media_source,
        last_value(article_title) over ({{partition_def}}) as article_title,
        last_value(article_summary) over ({{partition_def}}) as article_summary,
        last_value(article_author) over ({{partition_def}}) as article_author,
        last_value(article_tags) over ({{partition_def}}) as article_tags,
        last_value(article_medias) over ({{partition_def}}) as article_medias,
        first_value(article_publication_ts) over ({{partition_def}}) as article_publication_ts,
        last_value(article_publication_ts) over ({{partition_def}}) as article_modification_ts
    
    from base  
    
),

final as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key([
            'article_url',
            'media_source'
        ]) }} as article_sk, 
    
    from dedup

)

select * from final
order by article_publication_ts
