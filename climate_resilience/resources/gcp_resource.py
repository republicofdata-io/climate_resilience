import base64
import json
import os

from dagster import EnvVar
from dagster_gcp import BigQueryResource

# Decode the base64 encoded credentials and write them to a temporary file
AUTH_FILE = "/tmp/gcp_creds.json"
with open(AUTH_FILE, "w") as f:
    json.dump(
        json.loads(base64.b64decode(os.getenv("BIGQUERY_CREDENTIALS"))),
        f,
    )

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = AUTH_FILE  # Bundling up Dagster objects

# Resource to interact with the BigQuery database
gcp_resource = BigQueryResource(
    project=EnvVar("BIGQUERY_PROJECT_ID"),
)
