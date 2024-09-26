from dagster import load_assets_from_modules

from . import medias, x

# Load assets from package modules
bronze_assets = load_assets_from_modules(
    modules=[medias, x],
    key_prefix="bronze",
    group_name="bronze",
)
