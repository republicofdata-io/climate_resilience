import base64
import json
import os

from dagster import Definitions

from .assets import enrichments, medias, x
from .io_managers import bigquery_io_manager
from .jobs import (
    refresh_media_feeds_job,
    refresh_social_network_conversations_job,
    refresh_social_network_posts_job,
)
from .resources import supabase_resource, x_resource
from .schedules import (
    refresh_media_feeds_schedule,
    refresh_social_network_conversations_schedule,
    refresh_social_network_posts_schedule,
)

# Create temp file for GCP credentials
AUTH_FILE = "/tmp/gcp_creds.json"
with open(AUTH_FILE, "w") as f:
    json.dump(json.loads(base64.b64decode(str(os.getenv("BIGQUERY_CREDENTIALS")))), f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = AUTH_FILE


defs = Definitions(
    assets=[
        *medias.media_feed_assets,
        x.x_conversations,
        x.x_conversation_posts,
        enrichments.social_network_user_profile_geolocations,
    ],
    jobs=[
        refresh_media_feeds_job,
        refresh_social_network_conversations_job,
        refresh_social_network_posts_job,
    ],
    schedules=[
        refresh_media_feeds_schedule,
        refresh_social_network_conversations_schedule,
        refresh_social_network_posts_schedule,
    ],
    resources={
        "bigquery_io_manager": bigquery_io_manager,
        "supabase_resource": supabase_resource,
        "x_resource": x_resource,
    },
)
