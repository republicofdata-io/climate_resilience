import base64
import json
import os

from dagster import Definitions, load_assets_from_modules

from .assets import data_lake
from .assets.analytics import analytics_assets
from .assets.prototypes import investigative_reporter_ai_agent
from .io_managers import bigquery_io_manager
from .jobs import (
    refresh_analytics_job,
    refresh_media_feeds_job,
    refresh_narrative_enrichments_job,
    refresh_social_network_conversations_job,
    refresh_social_network_posts_job,
)
from .resources import dbt_resource, hex_resource, supabase_resource, x_resource
from .schedules import (
    refresh_analytics_schedule,
    refresh_media_feeds_schedule,
    refresh_narrative_enrichments_schedule,
    refresh_social_network_conversations_schedule,
    refresh_social_network_posts_schedule,
)

# Create temp file for GCP credentials
AUTH_FILE = "/tmp/gcp_creds.json"
with open(AUTH_FILE, "w") as f:
    json.dump(json.loads(base64.b64decode(str(os.getenv("BIGQUERY_CREDENTIALS")))), f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = AUTH_FILE


# Load assets from package modules
data_lake_assets = load_assets_from_modules(
    modules=[
        data_lake.medias,
        data_lake.x,
        data_lake.geolocation,
        data_lake.narratives,
    ],
    group_name="data_lake",
)

# Define the Dagster app
defs = Definitions(
    assets=[analytics_assets, *data_lake_assets, investigative_reporter_ai_agent],
    jobs=[
        refresh_analytics_job,
        refresh_media_feeds_job,
        refresh_social_network_conversations_job,
        refresh_social_network_posts_job,
        refresh_narrative_enrichments_job,
    ],
    schedules=[
        refresh_analytics_schedule,
        refresh_media_feeds_schedule,
        refresh_social_network_conversations_schedule,
        refresh_social_network_posts_schedule,
        refresh_narrative_enrichments_schedule,
    ],
    resources={
        "bigquery_io_manager": bigquery_io_manager,
        "dbt_resource": dbt_resource,
        "hex_resource": hex_resource,
        "supabase_resource": supabase_resource,
        "x_resource": x_resource,
    },
)
