from datetime import timedelta

from dagster import (
    RunRequest,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    schedule,
)

from ..jobs import (
    refresh_gold_job,
    refresh_media_feeds_job,
    refresh_narrative_enrichments_job,
    refresh_social_network_conversations_job,
    refresh_social_network_posts_job,
)
from ..partitions import three_hour_partition_def

refresh_media_feeds_schedule = build_schedule_from_partitioned_job(
    job=refresh_media_feeds_job
)

refresh_social_network_conversations_schedule = build_schedule_from_partitioned_job(
    refresh_social_network_conversations_job,
    minute_of_hour=15,
)


@schedule(
    job=refresh_social_network_posts_job,
    cron_schedule="30 */3 * * *",
)
def refresh_social_network_posts_schedule(context):
    execution_time = context.scheduled_execution_time
    partition_key = three_hour_partition_def.get_last_partition_key(
        current_time=execution_time - timedelta(minutes=30)
    )
    return RunRequest(partition_key=partition_key)


@schedule(
    job=refresh_narrative_enrichments_job,
    cron_schedule="45 */3 * * *",
)
def refresh_narrative_enrichments_schedule(context):
    execution_time = context.scheduled_execution_time
    partition_key = three_hour_partition_def.get_last_partition_key(
        current_time=execution_time - timedelta(minutes=45)
    )
    return RunRequest(partition_key=partition_key)


refresh_gold_schedule = ScheduleDefinition(
    job=refresh_gold_job, cron_schedule="0 7,19 * * *"
)
