with s_social_network_posts as (

    select * from {{ ref('int__social_network_posts') }}

),

s_conversation_classifications as (

    select * from {{ ref('stg__conversation_classifications') }}

),

s_conversation_event_summaries as (

    select * from {{ ref('stg__conversation_event_summaries') }}

),

base as (

    select distinct
        social_network_source,
        conversation_natural_key,
        media_source,
        article_url,
        min(post_creation_ts) as earliest_post_creation_ts,
    
    from s_social_network_posts

    group by 1, 2, 3, 4

),

merge_sources as (

  select 
    base.*,
    s_conversation_classifications.is_climate_conversation as is_climate_conversation,
    s_conversation_event_summaries.event_summary

  from base
  left join s_conversation_classifications
      on base.conversation_natural_key = s_conversation_classifications.conversation_natural_key
      and base.social_network_source = s_conversation_classifications.social_network_source
  left join s_conversation_event_summaries
      on base.conversation_natural_key = s_conversation_event_summaries.conversation_natural_key
      and base.social_network_source = s_conversation_event_summaries.social_network_source

),

dedup as (

    select distinct
        social_network_source,
        conversation_natural_key,
        first_value(media_source ignore nulls) over (partition by conversation_natural_key order by earliest_post_creation_ts) as media_source,
        first_value(article_url ignore nulls) over (partition by conversation_natural_key order by earliest_post_creation_ts) as article_url,
        first_value(earliest_post_creation_ts ignore nulls) over (partition by conversation_natural_key order by earliest_post_creation_ts) as earliest_post_creation_ts,
        first_value(is_climate_conversation ignore nulls) over (partition by conversation_natural_key order by earliest_post_creation_ts) as is_climate_conversation,
        first_value(event_summary ignore nulls) over (partition by conversation_natural_key order by earliest_post_creation_ts) as event_summary

    from merge_sources

)

select * from dedup
