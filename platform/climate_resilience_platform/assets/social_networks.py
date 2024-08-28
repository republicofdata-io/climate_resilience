import os
from datetime import datetime

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    TimeWindowPartitionMapping,
    asset,
)

from ..partitions import hourly_partition_def
from ..resources import SupabaseResource

media_article_columns = {
    "media": "string",
    "id": "string",
    "title": "string",
    "link": "string",
    "summary": "string",
    "author": "string",
    "tags": "string",
    "medias": "string",
    "published_ts": "datetime64[ns]",
}

post_columns = {
    "article_url": "string",
    "tweet_id": "int64",
    "tweet_created_at": "datetime64[ns]",
    "tweet_conversation_id": "int64",
    "tweet_text": "string",
    "tweet_public_metrics": "string",
    "author_id": "int64",
    "author_username": "string",
    "author_location": "string",
    "author_description": "string",
    "author_created_at": "datetime64[ns]",
    "author_public_metrics": "string",
    "partition_hour_utc_ts": "datetime64[ns]",
    "record_loading_ts": "datetime64[ns]",
}

# Get media feeds
supabase_resource = SupabaseResource(
    url=os.environ["SUPABASE_URL"], key=os.environ["SUPABASE_KEY"]
)
media_feeds = supabase_resource.get_media_feeds()
asset_ins = {
    f"{media_feed['slug']}_articles": AssetIn(
        AssetKey(["medias", str(media_feed["slug"]) + "_articles"]),
        partition_mapping=TimeWindowPartitionMapping(start_offset=-24),
    )
    for _, media_feed in media_feeds.iterrows()
}


@asset(
    name="x_conversations",
    key_prefix=["social_networks"],
    description="X conversations that mention this partition's article",
    io_manager_key="bigquery_io_manager",
    ins=asset_ins,
    partitions_def=hourly_partition_def,
    metadata={"partition_expr": "partition_hour_utc_ts"},
    output_required=False,
    compute_kind="python",
)
def x_conversations(context: AssetExecutionContext, **kwargs) -> None:
    # Get partition's time
    partition_time_str = context.partition_key
    partition_time = datetime.strptime(partition_time_str, "%Y-%m-%d-%H:%M")

    # Calculate start and end times for the scraping of the social network
    start_time = partition_time.isoformat(timespec="seconds") + "Z"
    end_time = (partition_time + pd.Timedelta(hours=1)).isoformat(
        timespec="seconds"
    ) + "Z"

    # Create an empty DataFrame that will hold all upstream articles
    articles_df = pd.DataFrame()
    articles_df = articles_df.reindex(columns=list(media_article_columns.keys()))

    # Iterate over kwargs and combine into a single dataframe of all articles
    for asset_key, asset_df in kwargs.items():
        articles_df = pd.concat([articles_df, asset_df])

    # Deduplicate the articles DataFrame
    articles_df = articles_df.drop_duplicates(subset=["link"])
    context.log.info(f"Scraping conversations for {len(articles_df)} articles.")

    # Create an empty DataFrame that will hold all conversations
    conversations_df = pd.DataFrame()
    conversations_df = articles_df.reindex(columns=list(post_columns.keys()))

    return None
