with source as (

    select * from {{ source('data_lake', 'conversation_classifications') }}

),

base as (

    select distinct
        cast(conversation_id as string) as conversation_natural_key,
        'x' as social_network_source,

        cast(classification as string) as classification,
        cast(partition_time as timestamp) as conversation_classification_partition_ts,
    
    from source

)

select * from base
order by conversation_classification_partition_ts, conversation_natural_key
