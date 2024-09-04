from datetime import datetime

import pandas as pd
from dagster import AssetIn, Output, TimeWindowPartitionMapping, asset

from ..partitions import hourly_partition_def


@asset(
    name="social_network_conversation_climate_classifications",
    key_prefix=["enrichments"],
    description="Classification of conversations as climate-related or not",
    io_manager_key="bigquery_io_manager",
    ins={
        "social_network_x_conversations": AssetIn(
            key=["social_networks", "x_conversations"],
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-13, end_offset=-13
            ),
        ),
        "social_network_x_conversation_posts": AssetIn(
            key=["social_networks", "x_conversation_posts"],
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-13, end_offset=0
            ),
        ),
    },
    partitions_def=hourly_partition_def,
    metadata={"partition_expr": "partition_hour_utc_ts"},
    compute_kind="openai",
)
def social_network_conversation_climate_classifications(
    context,
    social_network_x_conversations,
    social_network_x_conversation_posts,
) -> None:
    # Get partition's time
    partition_time_str = context.partition_key
    partition_time = datetime.strptime(partition_time_str, "%Y-%m-%d-%H:%M")

    # Assemble full conversations
    full_conversations = (
        (
            pd.merge(
                social_network_x_conversations,
                social_network_x_conversation_posts,
                how="left",
                on="tweet_conversation_id",
            )
        )
        .assign(
            tweet_id=lambda x: x["tweet_id_y"].combine_first(x["tweet_id_x"]),
            tweet_text=lambda x: x["tweet_text_y"].combine_first(x["tweet_text_x"]),
            tweet_created_at=lambda x: x["tweet_created_at_y"].combine_first(
                x["tweet_created_at_x"]
            ),
        )
        .loc[:, ["tweet_conversation_id", "tweet_id", "tweet_text", "tweet_created_at"]]
        .drop_duplicates()
    )

    # Get count of unique conversations
    unique_conversations = full_conversations["tweet_conversation_id"].nunique()

    context.log.info(
        f"Classifying {unique_conversations} social network conversation posts."
    )

    return None
