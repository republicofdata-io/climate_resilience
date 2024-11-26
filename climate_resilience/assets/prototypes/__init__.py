from dagster import load_assets_from_modules

from . import conversation_event_summary_agent

# Load assets from package modules
prototype_assets = load_assets_from_modules(
    modules=[
        conversation_event_summary_agent,
    ],
    key_prefix="prototypes",
    group_name="prototypes",
)
