with s_posts as (

  select
    cast(tweet_id as string) as post_id,
    cast(tweet_conversation_id as string) as conversation_id,
    cast(author_id as string) as author_id,
    author_username,
    author_description,
    article_url,
    tweet_text as post_text,
    tweet_created_at as post_creation_ts

  from  `phonic-biplane-420020.social_networks_0_3.x_conversation_posts`

),

s_user_geolocations as (

  select distinct
    cast(social_network_profile_id as string) as author_id,
    first_value(countryCode) over (partition by social_network_profile_id order by location_order, geolocation_ts RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as country_code,
    first_value(adminName1) over (partition by social_network_profile_id order by location_order, geolocation_ts RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as admin1_name
  
  from `phonic-biplane-420020.social_networks_0_3.social_network_user_profile_geolocations` 
  
  where adminName1 is not null
  and countryCode = 'US'

),

final as (

  select
    s_posts.post_id,
    s_posts.conversation_id,
    s_posts.author_id,
    s_posts.author_username,
    s_posts.author_description,
    s_user_geolocations.admin1_name,
    s_posts.article_url,
    s_posts.post_text,
    s_posts.post_creation_ts


  from s_posts
  inner join s_user_geolocations on s_posts.author_id = s_user_geolocations.author_id 

)

select * from final
