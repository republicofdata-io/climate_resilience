with source as (

    select * from {{ source('silver', 'conversation_event_summaries') }}

),

base as (

    select distinct
        cast(conversation_natural_key as string) as conversation_natural_key,
        'x' as social_network_source,
        cast(completeness_assessment as boolean) as is_complete,
        cast(research_cycles as integer) as research_cycles,
        cast(research_findings as string) as research_findings,
        cast(event_summary as string) as event_summary,

    from source

),

dedup as (
    
        select
            conversation_natural_key,
            social_network_source,
            is_complete,
            research_cycles,
            research_findings,
            event_summary,
            row_number() over (partition by conversation_natural_key order by conversation_natural_key) as row_number
    
        from base

)

select * from dedup
where row_number = 1
order by conversation_natural_key
