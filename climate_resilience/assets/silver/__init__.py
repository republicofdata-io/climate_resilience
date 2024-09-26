from dagster import load_assets_from_modules

from . import geolocation, narratives

# Load assets from package modules
silver_assets = load_assets_from_modules(
    modules=[geolocation, narratives],
    key_prefix="silver",
    group_name="silver",
)
