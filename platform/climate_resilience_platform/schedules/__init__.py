from dagster import build_schedule_from_partitioned_job

from ..jobs import refresh_media_feeds_job

refresh_media_feeds_schedule = build_schedule_from_partitioned_job(
    job=refresh_media_feeds_job
)
