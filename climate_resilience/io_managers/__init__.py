from dagster import EnvVar
from dagster_gcp_pandas import BigQueryPandasIOManager

bronze_io_manager = BigQueryPandasIOManager(
    project=EnvVar("BIGQUERY_PROJECT_ID"),
    dataset=EnvVar("BIGQUERY_BRONZE_DATASET"),
)

silver_io_manager = BigQueryPandasIOManager(
    project=EnvVar("BIGQUERY_PROJECT_ID"),
    dataset=EnvVar("BIGQUERY_SILVER_DATASET"),
)

gold_io_manager = BigQueryPandasIOManager(
    project=EnvVar("BIGQUERY_PROJECT_ID"),
    dataset=EnvVar("BIGQUERY_GOLD_DATASET"),
)
