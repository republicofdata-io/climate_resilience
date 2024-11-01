import json
from datetime import datetime

import pandas as pd
from dagster import AssetIn, Output, TimeWindowPartitionMapping, asset

from ...agents import conversation_classification_agent, post_association_agent
from ...partitions import three_hour_partition_def
from ...utils.conversations import assemble_conversations

conversation_classification_columns = {
    "conversation_id": "string",
    "classification": "string",
    "partition_time": "datetime64[ns]",
}

post_association_columns = {
    "post_id": "string",
    "discourse_type": "string",
    "partition_time": "datetime64[ns]",
}


@asset(
    name="conversation_classifications",
    description="Classification of conversations as climate-related or not",
    io_manager_key="silver_io_manager",
    ins={
        "x_conversations": AssetIn(
            key=["bronze", "x_conversations"],
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-4, end_offset=-4
            ),
        ),
        "x_conversation_posts": AssetIn(
            key=["bronze", "x_conversation_posts"],
            partition_mapping=TimeWindowPartitionMapping(start_offset=-4, end_offset=0),
        ),
    },
    partitions_def=three_hour_partition_def,
    metadata={"partition_expr": "partition_time"},
    compute_kind="LangGraph",
)
def conversation_classifications(
    context,
    x_conversations,
    x_conversation_posts,
):
    # Log upstream asset's partition keys
    context.log.info(
        f"Partition key range for x_conversations: {context.asset_partition_key_range_for_input('x_conversations')}"
    )
    context.log.info(
        f"Partition key range for x_conversation_posts: {context.asset_partition_key_range_for_input('x_conversation_posts')}"
    )

    # Get partition's time
    partition_time_str = context.partition_key
    partition_time = datetime.strptime(partition_time_str, "%Y-%m-%d-%H:%M")

    # Initialize DataFrame to store classifications
    conversation_classifications_df = pd.DataFrame()
    conversation_classifications_df = conversation_classifications_df.reindex(
        columns=list(conversation_classification_columns.keys())
    )
    conversation_classifications_df = conversation_classifications_df.astype(
        conversation_classification_columns
    )

    if not x_conversations.empty:
        # Assemble full conversations
        conversations_df = assemble_conversations(x_conversations, x_conversation_posts)

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

        # Iterate over all conversations and classify them
        for _, conversation_df in conversations_df.iterrows():
            conversation_dict = conversation_df.to_dict()
            conversation_json = json.dumps(conversation_dict)
            context.log.info(f"Classifying conversation: {conversation_json}")

            conversation_classifications_output = (
                conversation_classification_agent.invoke(
                    {"conversation_posts_json": conversation_json}
                )
            )
            new_classification = pd.DataFrame(
                [conversation_classifications_output.dict()]
            )
            context.log.info(f"Classification: {new_classification}")

            conversation_classifications_df = pd.concat(
                [conversation_classifications_df, new_classification], ignore_index=True
            )

        # Append partition time to DataFrame
        conversation_classifications_df["partition_time"] = partition_time

    # Return asset
    yield Output(
        value=conversation_classifications_df,
        metadata={
            "num_rows": conversation_classifications_df.shape[0],
        },
    )


@asset(
    name="post_narrative_associations",
    description="Associations between social network posts and narrative types",
    io_manager_key="silver_io_manager",
    ins={
        "x_conversations": AssetIn(
            key=["bronze", "x_conversations"],
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-4, end_offset=-4
            ),
        ),
        "x_conversation_posts": AssetIn(
            key=["bronze", "x_conversation_posts"],
            partition_mapping=TimeWindowPartitionMapping(start_offset=-4, end_offset=0),
        ),
        "conversation_classifications": AssetIn(
            key=["silver", "conversation_classifications"],
            partition_mapping=TimeWindowPartitionMapping(start_offset=0, end_offset=0),
        ),
    },
    partitions_def=three_hour_partition_def,
    metadata={"partition_expr": "partition_time"},
    compute_kind="LangGraph",
)
def post_narrative_associations(
    context,
    x_conversations,
    x_conversation_posts,
    conversation_classifications,
):
    # Log upstream asset's partition keys
    context.log.info(
        f"Partition key range for x_conversations: {context.asset_partition_key_range_for_input('x_conversations')}"
    )
    context.log.info(
        f"Partition key range for x_conversation_posts: {context.asset_partition_key_range_for_input('x_conversation_posts')}"
    )
    context.log.info(
        f"Partition key range for conversation_classifications: {context.asset_partition_key_range_for_input('conversation_classifications')}"
    )

    # Get partition's time
    partition_time_str = context.partition_key
    partition_time = datetime.strptime(partition_time_str, "%Y-%m-%d-%H:%M")

    # Initialize DataFrame to store classifications
    post_associations_df = pd.DataFrame()
    post_associations_df = post_associations_df.reindex(
        columns=list(post_association_columns.keys())
    )
    post_associations_df = post_associations_df.astype(post_association_columns)

    if not x_conversations.empty:
        # Assemble full conversations
        conversations_df = assemble_conversations(
            x_conversations,
            x_conversation_posts,
            conversation_classifications,
        )

        # Iterate over all conversations and classify them
        for _, conversation_df in conversations_df.iterrows():
            conversation_dict = conversation_df.to_dict()
            conversation_json = json.dumps(conversation_dict)
            context.log.info(f"Classifying conversation: {conversation_json}")

            try:
                post_associations_output = post_association_agent.invoke(
                    {"conversation_posts_json": conversation_json}
                )
                context.log.info(f"Associations: {post_associations_output}")

                for association in post_associations_output.post_associations:
                    new_row = {
                        "post_id": association.post_id,
                        "discourse_type": association.discourse,
                    }
                    post_associations_df = pd.concat(
                        [post_associations_df, pd.DataFrame([new_row])],
                        ignore_index=True,
                    )
            except Exception as e:
                print(f"Failed to associate posts")
                print(e)

        # Append partition time to DataFrame
        post_associations_df["partition_time"] = partition_time

    # Return asset
    yield Output(
        value=post_associations_df,
        metadata={
            "num_rows": post_associations_df.shape[0],
        },
    )
