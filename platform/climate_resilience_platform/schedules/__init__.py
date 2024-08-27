from dagster import ScheduleDefinition

from ..jobs import refresh_media_feeds_job

refresh_media_feeds_schedule = ScheduleDefinition(
    job=refresh_media_feeds_job, cron_schedule="5 * * * *"
)
