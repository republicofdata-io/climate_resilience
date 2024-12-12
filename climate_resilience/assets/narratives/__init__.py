from dagster import load_assets_from_modules

from . import event_summary, narratives

# Load assets from package modules
narratives_assets = load_assets_from_modules(
    modules=[narratives, event_summary],
    key_prefix="narratives",
    group_name="narratives",
)
