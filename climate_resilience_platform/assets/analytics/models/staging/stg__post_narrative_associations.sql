with source as (

    select * from {{ source('data_lake', 'post_narrative_associations') }}

),

base as (

    select distinct
        cast(post_id as string) as post_natural_key,
        
        cast(discourse_type as string) as discourse_type,
        cast(partition_time as timestamp) as discourse_type_partition_ts,
    
    from source

)

select * from base
order by discourse_type_partition_ts, post_natural_key
