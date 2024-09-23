with s_media_articles as (

    select * from {{ ref('int__media_articles') }}

), 

final as (

    select distinct
        {{ dbt_utils.generate_surrogate_key([
            'media_source',
            'article_url',
        ]) }} as media_article_pk, 

        article_url,
        media_source,

        article_title,
        article_summary,
        article_tags,
        article_author,
        article_medias,

        article_publication_ts,
        article_modification_ts
    
    from s_media_articles

)

select * from final
order by article_publication_ts
