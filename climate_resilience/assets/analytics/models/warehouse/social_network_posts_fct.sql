with s_social_network_posts as (

    select * from {{ ref('int__social_network_posts') }}

), 

final as (

    select distinct
        {{ dbt_utils.generate_surrogate_key([
            'social_network_source',
            'post_natural_key',
        ]) }} as social_network_post_pk,

        {{ dbt_utils.generate_surrogate_key([
            'social_network_source',
            'conversation_natural_key',
        ]) }} as social_network_conversation_fk,

        {{ dbt_utils.generate_surrogate_key([
            'social_network_source',
            'post_author_natural_key',
        ]) }} as social_network_user_profile_fk,

        conversation_natural_key,
        social_network_source,
        post_natural_key,

        post_url,
        post_text,
        post_type,
        discourse_category,
        discourse_sub_category,
        narrative,
        justification,

        post_creation_ts
    
    from s_social_network_posts

)

select * from final
order by post_creation_ts
