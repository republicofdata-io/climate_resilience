from dagster import load_assets_from_modules

from . import conversation_enhancements

# Load assets from package modules
prototype_assets = load_assets_from_modules(
    modules=[conversation_enhancements],
    key_prefix="prototypes",
    group_name="prototypes",
)
