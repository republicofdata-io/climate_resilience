import ast
import json
import os
import time
from datetime import datetime
from typing import List, TypedDict

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


class Post(TypedDict):
    article_url: str
    tweet_id: str
    tweet_created_at: datetime
    tweet_conversation_id: str
    tweet_text: str
    tweet_public_metrics: str
    author_id: str
    author_username: str
    author_location: str
    author_description: str
    author_created_at: datetime
    author_public_metrics: str
    partition_hour_utc_ts: datetime
    record_loading_ts: datetime


# Get media feeds
supabase_resource = SupabaseResource(
    url=os.environ["SUPABASE_URL"], key=os.environ["SUPABASE_KEY"]
)
media_feeds = supabase_resource.get_media_feeds()
asset_ins = {
    f"{media_feed['slug']}_articles": AssetIn(
        AssetKey(["media", str(media_feed["slug"]) + "_articles"]),
        partition_mapping=TimeWindowPartitionMapping(start_offset=-24),
    )
    for _, media_feed in media_feeds.iterrows()
}


@asset(
    name="x_conversations",
    description="X conversations that mention this partition's article",
    io_manager_key="social_networks_io_manager",
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

    # Iterate over kwargs and combine into a single dataframe of all articles
    for asset_key, asset_df in kwargs.items():
        articles_df = pd.concat([articles_df, asset_df])

    # Deduplicate the articles DataFrame
    articles_df = articles_df.drop_duplicates(subset=["link"])
    context.log.info(f"Scraping conversations for {len(articles_df)} articles.")

    # Create an empty DataFrame that will hold all conversations
    conversations_df = pd.DataFrame()
    conversations_df = pd.DataFrame(columns=Post.__annotations__.keys())

    # Iterate over the articles and search for tweets that mention the article
    failure_count = 0
    index = 0

    while index < len(articles_df):
        article_row = articles_df.iloc[index]
        try:
            search_term = (
                f'url:"{article_row["link"]}" -RT -is:retweet -is:reply lang:en'
            )

            # Get posts that mention the article and log consumption to table
            x_posts = x_resource.search(
                search_term,
                n_results=10,
                start_time=start_time,
                end_time=end_time,
            )

            # Process x_posts and convert each row to a Post dictionary
            posts: List[Post] = []

            for _, post_row in x_posts.iterrows():
                post: Post = {
                    "article_url": article_row["link"],
                    "tweet_id": str(post_row["tweet_id"]),
                    "tweet_created_at": pd.to_datetime(
                        post_row["tweet_created_at"]
                    ).tz_localize(None),
                    "tweet_conversation_id": str(post_row["tweet_conversation_id"]),
                    "tweet_text": post_row["tweet_text"],
                    "tweet_public_metrics": post_row["tweet_public_metrics"],
                    "author_id": str(post_row["author_id"]),
                    "author_username": post_row["author_username"],
                    "author_location": post_row.get(
                        "author_location", ""
                    ),  # Default to empty string
                    "author_description": post_row.get(
                        "author_description", ""
                    ),  # Default to empty string
                    "author_created_at": pd.to_datetime(
                        post_row["author_created_at"]
                    ).tz_localize(None),
                    "author_public_metrics": post_row["author_public_metrics"],
                    "partition_hour_utc_ts": partition_time,
                    "record_loading_ts": datetime.now(),
                }
                posts.append(post)

            # Convert the list of Post dictionaries to a DataFrame
            x_posts_df = pd.DataFrame(posts)

            # Concatenate the new x_posts to the DataFrame
            conversations_df = pd.concat(
                [conversations_df, x_posts_df], ignore_index=True
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
    io_manager_key="social_networks_io_manager",
    ins={
        "x_conversations": AssetIn(
            key=["social_networks", "x_conversations"],
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
    conversation_posts_df = pd.DataFrame(columns=Post.__annotations__.keys())

    # Deduplicate the x_conversations DataFrame
    x_conversations = x_conversations.drop_duplicates(subset=["tweet_conversation_id"])
    context.log.info(f"Checking metrics for {len(x_conversations)} conversations.")

    # Get latest metrics for the conversations Create an empty DataFrame with the specified columns and data types
    conversation_metrics_df = pd.DataFrame()
    conversation_metrics_df = pd.DataFrame(columns=Post.__annotations__.keys())

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
        article_row = active_conversations_df.iloc[index]
        try:
            search_term = f'conversation_id:{article_row["tweet_conversation_id"]} -RT -is:retweet lang:en'

            # Get posts that are replies to the conversation and log consumption to table
            x_conversation_posts = x_resource.search(
                search_term,
                n_results=10,
                start_time=start_time,
                end_time=end_time,
            )

            posts: List[Post] = []

            for _, post_row in x_conversation_posts.iterrows():
                post: Post = {
                    "article_url": article_row["article_url"],
                    "tweet_id": str(post_row["tweet_id"]),
                    "tweet_created_at": pd.to_datetime(
                        post_row["tweet_created_at"]
                    ).tz_localize(None),
                    "tweet_conversation_id": str(post_row["tweet_conversation_id"]),
                    "tweet_text": post_row["tweet_text"],
                    "tweet_public_metrics": post_row["tweet_public_metrics"],
                    "author_id": str(post_row["author_id"]),
                    "author_username": post_row["author_username"],
                    "author_location": post_row.get(
                        "author_location", ""
                    ),  # Default to empty string
                    "author_description": post_row.get(
                        "author_description", ""
                    ),  # Default to empty string
                    "author_created_at": pd.to_datetime(
                        post_row["author_created_at"]
                    ).tz_localize(None),
                    "author_public_metrics": post_row["author_public_metrics"],
                    "partition_hour_utc_ts": partition_time,
                    "record_loading_ts": datetime.now(),
                }
                posts.append(post)

            # Convert the list of Post dictionaries to a DataFrame
            x_conversation_posts = pd.DataFrame(posts)

            # Concatenate the new x_posts to the DataFrame
            conversation_posts_df = pd.concat(
                [conversation_posts_df, x_conversation_posts], ignore_index=True
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
