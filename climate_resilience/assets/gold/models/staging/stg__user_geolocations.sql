with source as (

    select * from {{ source('silver', 'user_geolocations') }}

),

base as (

    select distinct
        cast(social_network_profile_id as string) as social_network_profile_natural_key,

        'x' as social_network_source,
        cast(social_network_profile_username as string) as social_network_profile_username,
        cast(location as string) as social_network_profile_location_name,
        cast(location_order as int) as social_network_profile_location_order,
        
        cast(countryname as string) as social_network_profile_location_country_name,
        cast(countrycode as string) as social_network_profile_location_country_code,
        cast(adminname1 as string) as social_network_profile_location_admin1_name,
        cast(admincode1 as string) as social_network_profile_location_admin1_code,
        cast(latitude as float64) as social_network_profile_location_latitude,
        cast(longitude as float64) as social_network_profile_location_longitude,

        cast(geolocation_ts as timestamp) as social_network_profile_geolocation_ts
    
    from source

),

final as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key([
            'social_network_profile_natural_key',
            'social_network_source'
        ]) }} as social_network_user_profile_geolocation_sk, 
    
    from base

)

select * from final
order by social_network_profile_geolocation_ts
