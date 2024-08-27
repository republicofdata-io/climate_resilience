from dagster import AssetSelection, define_asset_job

from ..assets import medias
from ..partitions import hourly_partition_def

refresh_media_feeds_job = define_asset_job(
    name="refresh_media_feeds_job",
    selection=AssetSelection.assets(*medias.media_feed_assets),
    partitions_def=hourly_partition_def,
    tags={"dagster/max_runtime": 30 * 60},
)
