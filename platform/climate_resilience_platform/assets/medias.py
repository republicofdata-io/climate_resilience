import os

from dagster import AssetsDefinition, asset

from ..resources import SupabaseResource

# Get media feeds
supabase_resource = SupabaseResource(
    url=os.environ["SUPABASE_URL"], key=os.environ["SUPABASE_KEY"]
)
media_feeds = supabase_resource.get_media_feeds()


# Factory to create media feed assets
def build_media_feed_assets(media_name: str) -> AssetsDefinition:
    @asset(
        name=f"{media_name}_media_feed",
    )
    def _asset() -> None:
        pass

    return _asset


media_feed_assets = []
for _, media_feed in media_feeds.iterrows():
    media_feed_asset = build_media_feed_assets(str(media_feed["slug"]))
    media_feed_assets.append(media_feed_asset)
