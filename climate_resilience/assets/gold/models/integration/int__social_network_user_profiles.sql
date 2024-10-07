{% set geolocation_dedup_partition_def = 'partition by social_network_profile_natural_key order by social_network_profile_location_order, social_network_profile_geolocation_ts RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING' %}

with s_x_conversations as (

    select * from {{ ref('stg__x_conversations') }}

),

s_x_conversation_posts as (

    select * from {{ ref('stg__x_conversation_posts') }}

),

s_social_network_user_profile_geolocations as (

    select * from {{ ref('stg__user_geolocations') }}

),

merge_sources as (

    select
        social_network_source,
        post_author_natural_key,
        post_author,
        post_author_description,
        post_author_creation_ts,
        post_creation_ts
    
    from s_x_conversations

    union all

    select
        social_network_source,
        post_author_natural_key,
        post_author,
        post_author_description,
        post_author_creation_ts,
        post_creation_ts
    
    from s_x_conversation_posts

),

dedup_profiles as (

    select distinct
        social_network_source,
        post_author_natural_key as social_network_profile_natural_key,
        last_value(post_author) over (partition by post_author_natural_key order by post_author_creation_ts RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as social_network_profile_username,
        last_value(post_author_description) over (partition by post_author_natural_key order by post_author_creation_ts RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as social_network_profile_description,
        last_value(post_author_creation_ts) over (partition by post_author_natural_key order by post_author_creation_ts RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as social_network_profile_creation_ts
    
    from merge_sources

),

append_geolocations as (

    select
        dedup_profiles.social_network_source,
        dedup_profiles.social_network_profile_natural_key,
        dedup_profiles.social_network_profile_username,
        dedup_profiles.social_network_profile_description,
        dedup_profiles.social_network_profile_creation_ts,

        s_social_network_user_profile_geolocations.social_network_profile_location_name,
        s_social_network_user_profile_geolocations.social_network_profile_location_order,      
        s_social_network_user_profile_geolocations.social_network_profile_location_country_name,
        s_social_network_user_profile_geolocations.social_network_profile_location_country_code,
        s_social_network_user_profile_geolocations.social_network_profile_location_admin1_name,
        s_social_network_user_profile_geolocations.social_network_profile_location_admin1_code,
        s_social_network_user_profile_geolocations.social_network_profile_location_latitude,
        s_social_network_user_profile_geolocations.social_network_profile_location_longitude,
        s_social_network_user_profile_geolocations.social_network_profile_geolocation_ts
    
    from dedup_profiles
    left join s_social_network_user_profile_geolocations
        on dedup_profiles.social_network_source = s_social_network_user_profile_geolocations.social_network_source
        and dedup_profiles.social_network_profile_natural_key = s_social_network_user_profile_geolocations.social_network_profile_natural_key

),

dedup_geolocations as (

    select distinct
        social_network_source,
        social_network_profile_natural_key,
        social_network_profile_username,
        social_network_profile_description,

        first_value(social_network_profile_location_name ignore nulls) over ({{geolocation_dedup_partition_def}}) as social_network_profile_location_name,
      
        first_value(social_network_profile_location_country_name ignore nulls) over ({{geolocation_dedup_partition_def}}) as social_network_profile_location_country_name,
        first_value(social_network_profile_location_country_code ignore nulls) over ({{geolocation_dedup_partition_def}}) as social_network_profile_location_country_code,
        first_value(social_network_profile_location_admin1_name ignore nulls) over ({{geolocation_dedup_partition_def}}) as social_network_profile_location_admin1_name,
        first_value(social_network_profile_location_admin1_code ignore nulls) over ({{geolocation_dedup_partition_def}}) as social_network_profile_location_admin1_code,
        first_value(social_network_profile_location_latitude ignore nulls) over ({{geolocation_dedup_partition_def}}) as social_network_profile_location_latitude,
        first_value(social_network_profile_location_longitude ignore nulls) over ({{geolocation_dedup_partition_def}}) as social_network_profile_location_longitude,

        social_network_profile_creation_ts
    
    from append_geolocations

),

encode_h3_cells as (

    select
        *,
        `carto-os`.carto.H3_FROMLONGLAT(social_network_profile_location_longitude, social_network_profile_location_latitude, 3) as social_network_profile_location_h3_r3
    
    from dedup_geolocations

)

select * from encode_h3_cells
