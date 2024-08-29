from dagster import EnvVar
from dagster_gcp_pandas import BigQueryPandasIOManager

# BigQuery IO manager for media articles
bigquery_io_manager = BigQueryPandasIOManager(
    project=EnvVar("BIGQUERY_PROJECT_ID"),
    dataset=EnvVar("BIGQUERY_DATASET"),
)
