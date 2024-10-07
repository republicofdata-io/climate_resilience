with s_social_network_posts as (

    select * from {{ ref('int__social_network_posts') }}

),

s_conversation_classifications as (

    select * from {{ ref('stg__conversation_classifications') }}

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

final as (

  select 
    base.*,
    s_conversation_classifications.classification as classification,

  from base
  left join s_conversation_classifications
      on base.conversation_natural_key = s_conversation_classifications.conversation_natural_key
      and base.social_network_source = s_conversation_classifications.social_network_source

)

select * from final
