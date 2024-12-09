from pathlib import Path
from typing import Any, Mapping, Optional

from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets


# Custom dbt translator to dynamically set asset properties
class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]):
        asset_key = super().get_asset_key(dbt_resource_props)

        # Check if the resource type is not a source
        if dbt_resource_props["resource_type"] != "source":
            asset_key = asset_key.with_prefix("gold")

        return asset_key

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        return "gold"


# Build assets for each account connector view
@dbt_assets(
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    manifest=Path("climate_resilience/assets/gold/target", "manifest.json"),
)
def analytics_assets(context, dbt_resource: DbtCliResource):
    dbt_build_args = ["build"]

    yield from dbt_resource.cli(dbt_build_args, context=context).stream()
