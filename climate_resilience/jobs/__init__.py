from dagster import AssetKey, AssetSelection, define_asset_job

from ..assets.analytics import analytics_assets
from ..assets.media.media import media_feed_assets
from ..partitions import hourly_partition_def, three_hour_partition_def

# Job to refresh media assets
media_feed_asset_keys = [
    AssetKey(["media", *asset_def.key.path]) for asset_def in media_feed_assets
]
refresh_media_assets_job = define_asset_job(
    name="refresh_media_assets_job",
    selection=AssetSelection.assets(*media_feed_asset_keys),
    partitions_def=hourly_partition_def,
    tags={"dagster/max_runtime": 30 * 60},
)

# Job to refresh social network conversation assets
refresh_social_network_conversation_assets_job = define_asset_job(
    name="refresh_social_network_conversation_assets_job",
    selection=AssetSelection.assets(AssetKey(["social_networks", "x_conversations"])),
    partitions_def=hourly_partition_def,
    tags={"dagster/max_runtime": 30 * 60},
)

# Job to refresh social network post assets
refresh_social_network_post_assets_job = define_asset_job(
    name="refresh_social_network_post_assets_job",
    selection=AssetSelection.assets(
        AssetKey(["social_networks", "x_conversation_posts"]),
        AssetKey(["social_networks", "user_geolocations"]),
    ),
    partitions_def=three_hour_partition_def,
    tags={"dagster/max_runtime": 30 * 60},
)

# Job to refresh narrative assets
refresh_narrative_assets_job = define_asset_job(
    name="refresh_narrative_assets_job",
    selection=AssetSelection.assets(
        AssetKey(["narratives", "conversation_classifications"]),
        AssetKey(["narratives", "post_narrative_associations"]),
        AssetKey(["narratives", "conversation_event_summary"]),
    ),
    partitions_def=three_hour_partition_def,
    tags={"dagster/max_runtime": 30 * 60},
)

# Job to refresh analytics assets
refresh_analytics_assets_job = define_asset_job(
    name="refresh_analytics_assets_job",
    selection=AssetSelection.assets(
        analytics_assets,
    ),
    tags={"dagster/max_runtime": 30 * 60},
)
