import ast
import os
import time
from datetime import datetime

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Output,
    TimeWindowPartitionMapping,
    asset,
)

from ...partitions import hourly_partition_def, three_hour_partition_def
from ...resources.supabase_resource import SupabaseResource
from ...resources.x_resource import XResource, XResourceException

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
    "tweet_id": "string",
    "tweet_created_at": "datetime64[ns]",
    "tweet_conversation_id": "string",
    "tweet_text": "string",
    "tweet_public_metrics": "string",
    "author_id": "string",
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
        AssetKey(["bronze", str(media_feed["slug"]) + "_articles"]),
        partition_mapping=TimeWindowPartitionMapping(start_offset=-24),
    )
    for _, media_feed in media_feeds.iterrows()
}


@asset(
    name="x_conversations",
    description="X conversations that mention this partition's article",
    io_manager_key="bronze_io_manager",
    ins=asset_ins,
    partitions_def=hourly_partition_def,
    metadata={"partition_expr": "partition_hour_utc_ts"},
    output_required=False,
    compute_kind="python",
)
def x_conversations(context: AssetExecutionContext, x_resource: XResource, **kwargs):
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
    articles_df = articles_df.astype(media_article_columns)

    # Iterate over kwargs and combine into a single dataframe of all articles
    for asset_key, asset_df in kwargs.items():
        articles_df = pd.concat([articles_df, asset_df])

    # Deduplicate the articles DataFrame
    articles_df = articles_df.drop_duplicates(subset=["link"])
    context.log.info(f"Scraping conversations for {len(articles_df)} articles.")

    # Create an empty DataFrame that will hold all conversations
    conversations_df = pd.DataFrame()
    conversations_df = conversations_df.reindex(columns=list(post_columns.keys()))
    conversations_df = conversations_df.astype(post_columns)

    # Iterate over the articles and search for tweets that mention the article
    failure_count = 0
    index = 0

    while index < len(articles_df):
        row = articles_df.iloc[index]
        try:
            search_term = f'url:"{row["link"]}" -RT -is:retweet -is:reply lang:en'

            # Get posts that mention the article and log consumption to table
            x_posts = x_resource.search(
                search_term,
                n_results=10,
                start_time=start_time,
                end_time=end_time,
            )

            # Add article URL to the DataFrame
            x_posts["article_url"] = row["link"]

            # Remove timezone information from timestamp
            x_posts["tweet_created_at"] = pd.to_datetime(
                x_posts["tweet_created_at"]
            ).dt.tz_localize(None)
            x_posts["author_created_at"] = pd.to_datetime(
                x_posts["author_created_at"]
            ).dt.tz_localize(None)

            # Add partition_hour_utc_ts and current timestamp as new fields to the DataFrame
            x_posts["partition_hour_utc_ts"] = partition_time.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
            x_posts["record_loading_ts"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

            # Concatenate the new x_posts to the DataFrame
            conversations_df = pd.concat(
                [conversations_df, x_posts.astype(post_columns)]
            )

            # Reset failure count on success
            failure_count = 0
            index += 1
        except XResourceException as e:
            if e.status_code == 429:
                # Increment failure count and sleep
                failure_count += 1
                sleep_time = 2**failure_count

                if sleep_time > 900:
                    # Break out of the loop if the sleep time exceeds 900 seconds
                    break

                time.sleep(sleep_time)
            else:
                # If it's a different exception, raise it
                raise e

    # Return asset
    yield Output(
        value=conversations_df,
        metadata={
            "num_rows": conversations_df.shape[0],
        },
    )


@asset(
    name="x_conversation_posts",
    description="Posts within X conversations",
    io_manager_key="bronze_io_manager",
    ins={
        "x_conversations": AssetIn(
            key=["bronze", "x_conversations"],
            partition_mapping=TimeWindowPartitionMapping(start_offset=-12),
        )
    },
    partitions_def=three_hour_partition_def,
    metadata={"partition_expr": "partition_hour_utc_ts"},
    compute_kind="python",
)
def x_conversation_posts(
    context,
    x_conversations,
    x_resource: XResource,
):
    context.log.info(
        f"Partition key range for x_conversations: {context.asset_partition_key_range_for_input('x_conversations')}"
    )

    # Get partition's time
    partition_time_str = context.partition_key
    partition_time = datetime.strptime(partition_time_str, "%Y-%m-%d-%H:%M")

    # Calculate start and end times for the scraping of the social network
    start_time = partition_time.isoformat(timespec="seconds") + "Z"
    end_time = (partition_time + pd.Timedelta(hours=3)).isoformat(
        timespec="seconds"
    ) + "Z"

    # Create an empty DataFrame with the specified columns and data types
    conversation_posts_df = pd.DataFrame()
    conversation_posts_df = conversation_posts_df.reindex(
        columns=list(post_columns.keys())
    )
    conversation_posts_df = conversation_posts_df.astype(post_columns)

    # Deduplicate the x_conversations DataFrame
    x_conversations = x_conversations.drop_duplicates(subset=["tweet_conversation_id"])
    context.log.info(f"Checking metrics for {len(x_conversations)} conversations.")

    # Get latest metrics for the conversations Create an empty DataFrame with the specified columns and data types
    conversation_metrics_df = pd.DataFrame()
    conversation_metrics_df = conversation_metrics_df.reindex(
        columns=list(post_columns.keys())
    )
    conversation_metrics_df = conversation_metrics_df.astype(post_columns)

    # Batch requests in groups of 100
    for i in range(0, len(x_conversations), 100):
        batch = x_conversations.iloc[i : i + 100]
        tweet_ids = ",".join(batch["tweet_id"].astype(str))

        # Iterate over the conversations and get the latest metrics
        failure_count = 0
        index = 0
        batch__x_conversation_metrics = None
        try:
            batch__x_conversation_metrics = x_resource.get_tweets(tweet_ids)

            # Reset failure count on success
            failure_count = 0
            index += 1
        except XResourceException as e:
            if e.status_code == 429:
                # Increment failure count and sleep
                failure_count += 1
                sleep_time = 2**failure_count

                if sleep_time > 900:
                    # Break out of the loop if the sleep time exceeds 900 seconds
                    break

                time.sleep(sleep_time)
            else:
                # If it's a different exception, raise it
                raise e

        # Convert 'tweet_conversation_id'
        if batch__x_conversation_metrics is not None:
            batch__x_conversation_metrics["tweet_conversation_id"] = (
                batch__x_conversation_metrics["tweet_conversation_id"].astype(str)
            )
            batch["tweet_conversation_id"] = batch["tweet_conversation_id"].astype(str)

            # Concatenate the batch to the full dataframe of conversation metrics
            conversation_metrics_df = pd.concat(
                [
                    conversation_metrics_df,
                    batch__x_conversation_metrics,
                ]
            )

    # Convert social_network_x_conversations__new_metrics['tweet_public_metrics'] to a dictionary
    conversation_metrics_df["tweet_public_metrics"] = conversation_metrics_df[
        "tweet_public_metrics"
    ].apply(lambda x: ast.literal_eval(x))

    active_conversations_df = conversation_metrics_df[
        conversation_metrics_df["tweet_public_metrics"].apply(
            lambda x: x["reply_count"] > 0
        )
    ]
    context.log.info(
        f"Getting posts from {len(active_conversations_df)} active conversations."
    )

    # Iterate over the x_conversations and search for replies to the conversation
    failure_count = 0
    index = 0

    while index < len(active_conversations_df):
        row = active_conversations_df.iloc[index]
        try:
            search_term = f'conversation_id:{row["tweet_conversation_id"]} -RT -is:retweet lang:en'

            # Get posts that are replies to the conversation and log consumption to table
            x_conversation_posts = x_resource.search(
                search_term,
                n_results=10,
                start_time=start_time,
                end_time=end_time,
            )

            # Add article URL to the DataFrame
            x_conversation_posts["article_url"] = row["article_url"]

            # Remove timezone information from timestamp
            x_conversation_posts["tweet_created_at"] = pd.to_datetime(
                x_conversation_posts["tweet_created_at"]
            ).dt.tz_localize(None)

            x_conversation_posts["author_created_at"] = pd.to_datetime(
                x_conversation_posts["author_created_at"]
            ).dt.tz_localize(None)

            # Add partition_hour_utc_ts and current timestamp as new fields to the DataFrame
            x_conversation_posts["partition_hour_utc_ts"] = partition_time.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )

            x_conversation_posts["record_loading_ts"] = datetime.now().strftime(
                "%Y-%m-%dT%H:%M:%S"
            )

            # Concatenate the new x_conversation_posts to the DataFrame
            conversation_posts_df = pd.concat(
                [
                    conversation_posts_df,
                    x_conversation_posts.astype(post_columns),
                ]
            )

            # Reset failure count on success
            failure_count = 0
            index += 1
        except XResourceException as e:
            if e.status_code == 429:
                # Increment failure count and sleep
                failure_count += 1
                sleep_time = 2**failure_count

                if sleep_time > 900:
                    # Break out of the loop if the sleep time exceeds 900 seconds
                    break

                time.sleep(sleep_time)
            else:
                # If it's a different exception, raise it
                raise e

    # Return asset
    yield Output(
        value=conversation_posts_df,
        metadata={
            "num_rows": conversation_posts_df.shape[0],
        },
    )
