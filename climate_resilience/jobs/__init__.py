from dagster import AssetKey, AssetSelection, define_asset_job

from ..assets.bronze import medias
from ..assets.gold import gold_assets
from ..partitions import hourly_partition_def, three_hour_partition_def

# Job to refresh media assets
media_feed_asset_keys = [
    AssetKey(["bronze", *asset_def.key.path]) for asset_def in medias.media_feed_assets
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
    selection=AssetSelection.assets(AssetKey(["bronze", "x_conversations"])),
    partitions_def=hourly_partition_def,
    tags={"dagster/max_runtime": 30 * 60},
)

# Job to refresh social network post assets
refresh_social_network_post_assets_job = define_asset_job(
    name="refresh_social_network_post_assets_job",
    selection=AssetSelection.assets(
        AssetKey(["bronze", "x_conversation_posts"]),
        AssetKey(["silver", "user_geolocations"]),
    ),
    partitions_def=three_hour_partition_def,
    tags={"dagster/max_runtime": 30 * 60},
)

# Job to refresh narrative assets
refresh_narrative_assets_job = define_asset_job(
    name="refresh_narrative_assets_job",
    selection=AssetSelection.assets(
        AssetKey(["silver", "conversation_classifications"]),
        AssetKey(["silver", "post_narrative_associations"]),
    ),
    partitions_def=three_hour_partition_def,
    tags={"dagster/max_runtime": 30 * 60},
)

# Job to refresh gold assets
refresh_gold_assets_job = define_asset_job(
    name="refresh_gold_assets_job",
    selection=AssetSelection.assets(
        gold_assets,
    ),
    tags={"dagster/max_runtime": 30 * 60},
)

# Job to refresh prototype assets
refresh_prototype_assets_job = define_asset_job(
    name="refresh_prototype_assets_job",
    selection=AssetSelection.assets(
        AssetKey(["prototypes", "investigative_reporter_ai_agent"])
    ),
    tags={"dagster/max_runtime": 30 * 60},
)
