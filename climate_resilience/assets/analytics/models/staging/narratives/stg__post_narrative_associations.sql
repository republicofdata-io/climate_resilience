with source as (

    select * from {{ source('narratives', 'post_narrative_associations') }}

),

base as (

    select distinct
        cast(post_id as string) as post_natural_key,
        'x' as social_network_source,
        
        cast(post_type as string) as post_type,
        cast(discourse_category as string) as discourse_category,
        cast(discourse_sub_category as string) as discourse_sub_category,
        cast(narrative as string) as narrative,
        cast(justification as string) as justification,
        cast(confidence as float64) as confidence,
        cast(partition_time as timestamp) as partition_ts,
    
    from source

)

select * from base
order by partition_ts, post_natural_key
