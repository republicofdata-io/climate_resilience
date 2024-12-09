import os

from dagster import file_relative_path
from dagster_dbt import DbtCliResource

from .supabase_resource import SupabaseResource
from .x_resource import XResource

supabase_resource = SupabaseResource(
    url=os.environ["SUPABASE_URL"], key=os.environ["SUPABASE_KEY"]
)

x_resource = XResource(x_bearer_token=os.environ.get("X_BEARER_TOKEN", ""))

dbt_resource = DbtCliResource(
    project_dir=file_relative_path(__file__, "../assets/analytics/"),
    profiles_dir=file_relative_path(__file__, "../assets/analytics/"),
    profile="climate_resilience_analytics",
    target="analytics",
)
