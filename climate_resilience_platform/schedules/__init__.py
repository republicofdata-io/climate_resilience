from dagster import build_schedule_from_partitioned_job

from ..jobs import (
    refresh_media_feeds_job,
    refresh_social_network_conversations_job,
    refresh_social_network_posts_job,
)

refresh_media_feeds_schedule = build_schedule_from_partitioned_job(
    job=refresh_media_feeds_job
)

refresh_social_network_conversations_schedule = build_schedule_from_partitioned_job(
    refresh_social_network_conversations_job,
    minute_of_hour=15,
)

refresh_social_network_posts_schedule = build_schedule_from_partitioned_job(
    refresh_social_network_posts_job,
)
