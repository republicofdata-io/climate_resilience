with source as (

    select * from {{ source('narratives', 'conversation_event_summaries') }}

),

base as (

    select distinct
        cast(conversation_id as string) as conversation_natural_key,
        'x' as social_network_source,
        cast(research_cycles as integer) as research_cycles,
        cast(event_summary as string) as event_summary,

    from source

),

dedup as (
    
        select
            conversation_natural_key,
            social_network_source,
            research_cycles,
            event_summary,
            row_number() over (partition by conversation_natural_key order by conversation_natural_key) as row_number
    
        from base

)

select * from dedup
where row_number = 1
order by conversation_natural_key
