from pathlib import Path
from typing import Any, Mapping, Optional

from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets


# Custom dbt translator to dynamically set asset properties
class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        return "analytics"


# Build assets for each account connector view
@dbt_assets(
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    manifest=Path(
        "climate_resilience_platform/assets/analytics/target", "manifest.json"
    ),
)
def analytics_assets(context, dbt_resource: DbtCliResource):
    dbt_build_args = ["build"]

    yield from dbt_resource.cli(dbt_build_args, context=context).stream()
