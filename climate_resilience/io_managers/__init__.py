from dagster import EnvVar
from dagster_gcp_pandas import BigQueryPandasIOManager

media_io_manager = BigQueryPandasIOManager(
    project=EnvVar("BIGQUERY_PROJECT_ID"),
    dataset=EnvVar("BIGQUERY_MEDIA_DATASET"),
)

social_networks_io_manager = BigQueryPandasIOManager(
    project=EnvVar("BIGQUERY_PROJECT_ID"),
    dataset=EnvVar("BIGQUERY_SOCIAL_NETWORKS_DATASET"),
)

narratives_io_manager = BigQueryPandasIOManager(
    project=EnvVar("BIGQUERY_PROJECT_ID"),
    dataset=EnvVar("BIGQUERY_NARRATIVES_DATASET"),
)

analytics_io_manager = BigQueryPandasIOManager(
    project=EnvVar("BIGQUERY_PROJECT_ID"),
    dataset=EnvVar("BIGQUERY_ANALYTICS_DATASET"),
)
