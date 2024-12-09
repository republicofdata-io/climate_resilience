from dagster import load_assets_from_modules

from . import geolocation, x

# Load assets from package modules
social_networks_assets = load_assets_from_modules(
    modules=[x, geolocation],
    key_prefix="social_networks",
    group_name="social_networks",
)
