from datetime import datetime, timedelta

from dagster import HourlyPartitionsDefinition

# Hourly partition
hourly_partition_def = HourlyPartitionsDefinition(
    start_date=datetime.now() - timedelta(days=29),
)
