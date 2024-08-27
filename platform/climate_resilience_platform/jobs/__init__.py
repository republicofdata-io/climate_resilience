from dagster import AssetSelection, define_asset_job

from ..assets import medias

refresh_media_feeds_job = define_asset_job(
    name="refresh_media_feeds_job",
    selection=AssetSelection.assets(*medias.media_feed_assets),
    tags={"dagster/max_runtime": 30 * 60},
)
