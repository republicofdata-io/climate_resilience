from dagster import AssetSelection, define_asset_job

from ..assets import medias, social_networks, x
from ..partitions import hourly_partition_def, three_hour_partition_def

refresh_media_feeds_job = define_asset_job(
    name="refresh_media_feeds_job",
    selection=AssetSelection.assets(*medias.media_feed_assets),
    partitions_def=hourly_partition_def,
    tags={"dagster/max_runtime": 30 * 60},
)

refresh_social_network_conversations_job = define_asset_job(
    name="refresh_social_network_conversations_job",
    selection=AssetSelection.assets(
        x.x_conversations,
    ),
    partitions_def=hourly_partition_def,
    tags={"dagster/max_runtime": 30 * 60},
)

refresh_social_network_posts_job = define_asset_job(
    name="refresh_social_network_posts_job",
    selection=AssetSelection.assets(
        x.x_conversation_posts,
        social_networks.social_network_user_profile_geolocations,
    ),
    partitions_def=three_hour_partition_def,
    tags={"dagster/max_runtime": 30 * 60},
)
