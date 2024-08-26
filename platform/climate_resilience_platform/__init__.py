from dagster import Definitions, load_assets_from_modules

from .assets.medias import media_feed_assets
from .resources import supabase_resource

defs = Definitions(
    assets=[*media_feed_assets],
    resources={
        "supabase_resource": supabase_resource,
    },
)
