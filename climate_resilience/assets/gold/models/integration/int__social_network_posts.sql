with s_x_conversations as (

    select * from {{ ref('stg__x_conversations') }}

),

s_x_conversation_posts as (

    select * from {{ ref('stg__x_conversation_posts') }}

),

s_post_narrative_associations as (

    select * from {{ ref('stg__post_narrative_associations') }}

),

merge_sources as (

    select
        s_x_conversations.conversation_natural_key,
        s_x_conversations.post_natural_key,
        s_x_conversations.social_network_source,
        
        s_x_conversations.post_author_natural_key,

        s_x_conversations.article_url,
        s_x_conversations.media_source,
        s_x_conversations.post_url,
        s_x_conversations.post_text,
        s_x_conversations.post_author,
        s_x_conversations.post_author_location,
        s_x_conversations.post_author_description,
        s_x_conversations.post_metrics,
        s_x_conversations.post_author_metrics,

        s_x_conversations.post_creation_ts,
        s_x_conversations.post_author_creation_ts,
        s_x_conversations.article_publication_partition_ts,
        s_x_conversations.record_loading_ts
    
    from s_x_conversations

    union all

    select
        s_x_conversation_posts.conversation_natural_key,
        s_x_conversation_posts.post_natural_key,
        s_x_conversation_posts.social_network_source,
       
        s_x_conversation_posts.post_author_natural_key,

        s_x_conversation_posts.article_url,
        s_x_conversation_posts.media_source,
        s_x_conversation_posts.post_url,
        s_x_conversation_posts.post_text,
        s_x_conversation_posts.post_author,
        s_x_conversation_posts.post_author_location,
        s_x_conversation_posts.post_author_description,
        s_x_conversation_posts.post_metrics,
        s_x_conversation_posts.post_author_metrics,

        s_x_conversation_posts.post_creation_ts,
        s_x_conversation_posts.post_author_creation_ts,
        s_x_conversation_posts.article_publication_partition_ts,
        s_x_conversation_posts.record_loading_ts
    
    from s_x_conversation_posts

),

dedup as (

    select
        *,
        row_number() over (partition by post_natural_key order by record_loading_ts desc) as row_num
    from merge_sources

),

final as (

    select
        dedup.*,
        s_post_narrative_associations.discourse_type,
        s_post_narrative_associations.narrative

    from dedup
    left join s_post_narrative_associations
        on dedup.post_natural_key = s_post_narrative_associations.post_natural_key
        and dedup.social_network_source = s_post_narrative_associations.social_network_source

)

select * from final
order by post_creation_ts
