from dagster import AssetSelection, define_asset_job

from ..assets import medias, social_networks
from ..partitions import hourly_partition_def

refresh_media_feeds_job = define_asset_job(
    name="refresh_media_feeds_job",
    selection=AssetSelection.assets(*medias.media_feed_assets),
    partitions_def=hourly_partition_def,
    tags={"dagster/max_runtime": 30 * 60},
)

refresh_social_network_conversations_job = define_asset_job(
    name="refresh_social_network_conversations_job",
    selection=AssetSelection.assets(
        social_networks.x_conversations,
    ),
    partitions_def=hourly_partition_def,
    tags={"dagster/max_runtime": 30 * 60},
)

refresh_social_network_posts_job = define_asset_job(
    name="refresh_social_network_posts_job",
    selection=AssetSelection.assets(
        social_networks.x_conversation_posts,
    ),
    partitions_def=hourly_partition_def,
    tags={"dagster/max_runtime": 30 * 60},
)
