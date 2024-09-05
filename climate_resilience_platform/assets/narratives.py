import json
import re
from datetime import datetime

import pandas as pd
from dagster import AssetIn, Output, TimeWindowPartitionMapping, asset

from ..agents import conversation_classification_agent
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
):
    # Get partition's time
    partition_time_str = context.partition_key
    partition_time = datetime.strptime(partition_time_str, "%Y-%m-%d-%H:%M")

    # Assemble full conversations
    conversations_df = (
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
        .assign(
            tweet_created_at=lambda df: df["tweet_created_at"].apply(
                lambda ts: ts.isoformat() if pd.notnull(ts) else None
            )
        )
        .loc[:, ["tweet_conversation_id", "tweet_id", "tweet_text", "tweet_created_at"]]
        .drop_duplicates()
        .sort_values(by=["tweet_conversation_id", "tweet_created_at"])
    )

    # Remove user mentions from tweet_text
    conversations_df["tweet_text"] = conversations_df["tweet_text"].apply(
        lambda x: re.sub(r"@\w+", "", x).strip()
    )

    # Group by tweet_conversation_id and aggregate tweet_texts into a list ordered by tweet_created_at
    conversations_df = (
        conversations_df.groupby("tweet_conversation_id")
        .apply(
            lambda x: x.sort_values("tweet_created_at")[
                ["tweet_id", "tweet_created_at", "tweet_text"]
            ].to_dict(orient="records")
        )
        .reset_index(name="posts")
    )

    context.log.info(
        f"Classifying {len(conversations_df)} social network conversation posts."
    )

    # Initialize DataFrame to store classifications
    conversation_classifications_df = pd.DataFrame()

    # Iterate over all conversations and classify them
    for _, conversation_df in conversations_df.iterrows():
        conversation_dict = conversation_df.to_dict()
        conversation_json = json.dumps(conversation_dict)
        context.log.info(f"Classifying conversation: {conversation_json}")

        conversation_classifications_output = conversation_classification_agent.invoke(
            {"conversation_posts_json": conversation_json}
        )
        new_classification = pd.DataFrame([conversation_classifications_output.dict()])
        context.log.info(f"Classification: {new_classification}")

        conversation_classifications_df = pd.concat(
            [conversation_classifications_df, new_classification], ignore_index=True
        )

    # Merge full conversations
    conversation_classifications_df = pd.merge(
        conversation_classifications_df,
        social_network_x_conversations,
        left_on="conversation_id",
        right_on="tweet_conversation_id",
    )

    # Return asset
    yield Output(
        value=conversation_classifications_df,
        metadata={
            "num_rows": conversation_classifications_df.shape[0],
        },
    )
