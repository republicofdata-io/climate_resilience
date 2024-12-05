from datetime import timedelta

from dagster import (
    RunRequest,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    schedule,
)

from ..jobs import (
    refresh_gold_assets_job,
    refresh_media_assets_job,
    refresh_narrative_assets_job,
    refresh_social_network_conversation_assets_job,
    refresh_social_network_post_assets_job,
)
from ..partitions import three_hour_partition_def

refresh_media_assets_schedule = build_schedule_from_partitioned_job(
    job=refresh_media_assets_job
)

refresh_social_network_conversation_assets_schedule = (
    build_schedule_from_partitioned_job(
        refresh_social_network_conversation_assets_job,
        minute_of_hour=10,
    )
)


@schedule(
    job=refresh_social_network_post_assets_job,
    cron_schedule="20 */3 * * *",
)
def refresh_social_network_post_assets_schedule(context):
    execution_time = context.scheduled_execution_time
    partition_key = three_hour_partition_def.get_last_partition_key(
        current_time=execution_time - timedelta(minutes=30)
    )
    return RunRequest(partition_key=partition_key)


@schedule(
    job=refresh_narrative_assets_job,
    cron_schedule="45 */3 * * *",
)
def refresh_narrative_assets_schedule(context):
    execution_time = context.scheduled_execution_time
    partition_key = three_hour_partition_def.get_last_partition_key(
        current_time=execution_time - timedelta(minutes=45)
    )
    return RunRequest(partition_key=partition_key)


refresh_gold_assets_schedule = ScheduleDefinition(
    job=refresh_gold_assets_job, cron_schedule="0 7,19 * * *"
)
