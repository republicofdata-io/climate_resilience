from datetime import datetime, timedelta

from dagster import HourlyPartitionsDefinition, TimeWindowPartitionsDefinition

# Hourly partition
hourly_partition_def = HourlyPartitionsDefinition(
    start_date=datetime.now() - timedelta(days=90),
)

# 3 hour partition
three_hour_partition_def = TimeWindowPartitionsDefinition(
    start=datetime.now() - timedelta(days=90),
    cron_schedule="0 */3 * * *",
    fmt="%Y-%m-%d-%H:%M",
)
