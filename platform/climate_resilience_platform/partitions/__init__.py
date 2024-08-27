from dagster import HourlyPartitionsDefinition

from datetime import datetime
from datetime import datetime, timedelta


# Hourly partition
hourly_partition_def = HourlyPartitionsDefinition(
    start_date=datetime.now() - timedelta(days=29),
)
