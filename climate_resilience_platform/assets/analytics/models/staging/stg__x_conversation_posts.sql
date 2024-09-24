{% set partition_def = 'partition by post_natural_key, social_network_source order by record_loading_ts RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING' %}

with source as (

    select * from {{ source('data_lake', 'x_conversation_posts') }}

),

base as (

    select distinct
        cast(tweet_id as string) as post_natural_key,
        'x' as social_network_source,
        
        cast(tweet_conversation_id as string) as conversation_natural_key,
        cast(author_id as string) as post_author_natural_key,

        cast(article_url as string) as article_url,
        'nytimes' as media_source,
        'https://twitter.com/' || cast(author_username as string) || '/status/' || cast(tweet_id as string) as post_url,
        cast(tweet_text as string) as post_text,
        cast(author_username as string) as post_author,
        cast(author_location as string) as post_author_location,
        cast(author_description as string) as post_author_description,
        cast(tweet_public_metrics as string) as post_metrics,
        cast(author_public_metrics as string) as post_author_metrics,

        cast(tweet_created_at as timestamp) as post_creation_ts,
        cast(author_created_at as timestamp) as post_author_creation_ts,
        cast(partition_hour_utc_ts as timestamp) as article_publication_partition_ts,
        cast(record_loading_ts as timestamp) as record_loading_ts
    
    from source

),

final as (
    
    select distinct
        post_natural_key,
        social_network_source,
        last_value(conversation_natural_key) over ({{partition_def}}) as conversation_natural_key,
        last_value(post_author_natural_key) over ({{partition_def}}) as post_author_natural_key,
        last_value(article_url) over ({{partition_def}}) as article_url,
        last_value(media_source) over ({{partition_def}}) as media_source,
        last_value(post_url) over ({{partition_def}}) as post_url,
        last_value(post_text) over ({{partition_def}}) as post_text,
        last_value(post_author) over ({{partition_def}}) as post_author,
        last_value(post_author_location) over ({{partition_def}}) as post_author_location,
        last_value(post_author_description) over ({{partition_def}}) as post_author_description,
        last_value(post_metrics) over ({{partition_def}}) as post_metrics,
        last_value(post_author_metrics) over ({{partition_def}}) as post_author_metrics,
        last_value(post_creation_ts) over ({{partition_def}}) as post_creation_ts,
        last_value(post_author_creation_ts) over ({{partition_def}}) as post_author_creation_ts,
        last_value(article_publication_partition_ts) over ({{partition_def}}) as article_publication_partition_ts,
        last_value(record_loading_ts) over ({{partition_def}}) as record_loading_ts
    
    from base  
    
)

select * from final
order by post_creation_ts
