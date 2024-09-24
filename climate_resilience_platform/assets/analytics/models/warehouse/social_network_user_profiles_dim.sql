with s_social_network_user_profiles as (

    select * from {{ ref('int__social_network_user_profiles') }}

), 

final as (

    select distinct
        {{ dbt_utils.generate_surrogate_key([
            'social_network_source',
            'social_network_profile_natural_key',
        ]) }} as social_network_user_profile_pk,

        social_network_source,
        social_network_profile_natural_key,
        
        social_network_profile_username,
        social_network_profile_description,
        social_network_profile_location_name,
        social_network_profile_location_country_name,
        social_network_profile_location_country_code,
        social_network_profile_location_admin1_name,
        social_network_profile_location_admin1_code,
        social_network_profile_location_latitude,
        social_network_profile_location_longitude,
        social_network_profile_location_h3_r3,

        social_network_profile_creation_ts
    
    from s_social_network_user_profiles

)

select * from final
