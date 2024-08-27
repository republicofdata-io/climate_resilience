import base64
import json
import os

from dagster import Definitions

from .assets.medias import media_feed_assets
from .io_managers import bigquery_io_manager
from .resources import supabase_resource

# Create temp file for GCP credentials
AUTH_FILE = "/tmp/gcp_creds.json"
with open(AUTH_FILE, "w") as f:
    json.dump(json.loads(base64.b64decode(str(os.getenv("BIGQUERY_CREDENTIALS")))), f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = AUTH_FILE


defs = Definitions(
    assets=[*media_feed_assets],
    resources={
        "bigquery_io_manager": bigquery_io_manager,
        "supabase_resource": supabase_resource,
    },
)
