with s_social_network_conversations as (

    select * from {{ ref('int__social_network_conversations') }}

), 

final as (

    select distinct
        {{ dbt_utils.generate_surrogate_key([
            'social_network_source',
            'conversation_natural_key',
        ]) }} as social_network_conversation_pk,

        {{ dbt_utils.generate_surrogate_key([
            'media_source',
            'article_url',
        ]) }} as media_article_fk,

        social_network_source,
        conversation_natural_key,
        classification,
        event_summary,
        earliest_post_creation_ts,
    
    from s_social_network_conversations

)

select * from final
order by earliest_post_creation_ts
