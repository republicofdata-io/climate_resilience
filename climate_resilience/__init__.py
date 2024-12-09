import base64
import json
import os

from dagster import Definitions

from .assets.analytics import analytics_assets
from .assets.media import media_assets
from .assets.narratives import narratives_assets
from .assets.social_networks import social_networks_assets
from .io_managers import bronze_io_manager, gold_io_manager, silver_io_manager
from .jobs import (
    refresh_gold_assets_job,
    refresh_media_assets_job,
    refresh_narrative_assets_job,
    refresh_social_network_conversation_assets_job,
    refresh_social_network_post_assets_job,
)
from .resources import dbt_resource, gcp_resource, supabase_resource, x_resource
from .schedules import (
    refresh_gold_assets_schedule,
    refresh_media_assets_schedule,
    refresh_narrative_assets_schedule,
    refresh_social_network_conversation_assets_schedule,
    refresh_social_network_post_assets_schedule,
)

# Create temp file for GCP credentials
AUTH_FILE = "/tmp/gcp_creds.json"
with open(AUTH_FILE, "w") as f:
    json.dump(json.loads(base64.b64decode(str(os.getenv("BIGQUERY_CREDENTIALS")))), f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = AUTH_FILE


# Define the Dagster app
defs = Definitions(
    assets=[
        *media_assets,
        *social_networks_assets,
        *narratives_assets,
        analytics_assets,
    ],
    jobs=[
        refresh_gold_assets_job,
        refresh_media_assets_job,
        refresh_social_network_conversation_assets_job,
        refresh_social_network_post_assets_job,
        refresh_narrative_assets_job,
    ],
    schedules=[
        refresh_gold_assets_schedule,
        refresh_media_assets_schedule,
        refresh_social_network_conversation_assets_schedule,
        refresh_social_network_post_assets_schedule,
        refresh_narrative_assets_schedule,
    ],
    resources={
        "bronze_io_manager": bronze_io_manager,
        "silver_io_manager": silver_io_manager,
        "gold_io_manager": gold_io_manager,
        "dbt_resource": dbt_resource,
        "gcp_resource": gcp_resource,
        "supabase_resource": supabase_resource,
        "x_resource": x_resource,
    },
)
