from dagster import load_assets_from_modules

from . import media

# Load assets from package modules
media_assets = load_assets_from_modules(
    modules=[media],
    key_prefix="media",
    group_name="media",
)
