from datetime import datetime
from typing import TypedDict

import pandas as pd
from dagster import AssetIn, Output, TimeWindowPartitionMapping, asset
from dagster_hex.resources import DEFAULT_POLL_INTERVAL
from dagster_hex.types import HexOutput

from ...partitions import three_hour_partition_def
from ...resources.hex_resource import ConfigurableHexResource


class ConversationBrief(TypedDict):
    conversation_id: str
    run_id: str
    run_url: str
    trace_id: str
    partition_time: datetime


@asset(
    name="investigative_reporter_ai_agent",
    description="Investigative Reporter AI Agent",
    io_manager_key="prototypes_io_manager",
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
    output_required=False,
    compute_kind="Hex",
)
def investigative_reporter_ai_agent(
    context,
    hex_resource: ConfigurableHexResource,
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

    context.log.info(f"Number of conversations: {len(x_conversations)}")
    conversation_brief_outputs = []

    if not x_conversations.empty:
        # Initialize Hex client
        hex_client = hex_resource.create_client()

        for _, conversation in x_conversations.iterrows():
            # Conversation's tweet text
            conversation_list = [conversation["tweet_text"]]

            for _, post in x_conversation_posts.iterrows():
                if (
                    post["tweet_conversation_id"]
                    == conversation["tweet_conversation_id"]
                ):
                    conversation_list.append(post["tweet_text"])

            # Package conversation into a markdown string
            conversation_markdown = f"# {conversation['tweet_conversation_id']}\n\n"
            conversation_markdown += "\n".join(
                [f"- {tweet}" for tweet in conversation_list]
            )

            context.log.info(
                f"Number of posts for conversation {conversation['tweet_conversation_id']}: {len(conversation_list)}"
            )

            if len(conversation_list) >= 3:
                context.log.info(
                    f"Launching Investigative Reporter AI Agent for conversation {conversation['tweet_conversation_id']}"
                )
                context.log.info(f"Conversation: {conversation_markdown}")

                hex_output: HexOutput = hex_client.run_and_poll(
                    project_id="00c977d2-e2c7-43a0-abfc-3d466dbad3c1",
                    inputs={
                        "conversation_id": conversation["tweet_conversation_id"],
                        "conversation_markdown": conversation_markdown,
                    },
                    kill_on_timeout=True,
                    poll_interval=DEFAULT_POLL_INTERVAL,
                    poll_timeout=None,
                )

                conversation_brief_output = ConversationBrief(
                    conversation_id=conversation["tweet_conversation_id"],
                    run_id=hex_output.run_response["runId"],
                    run_url=hex_output.run_response["runUrl"],
                    trace_id=hex_output.run_response["traceId"],
                    partition_time=partition_time,
                )

                conversation_brief_outputs.append(conversation_brief_output)

    if conversation_brief_outputs:
        yield Output(
            value=pd.DataFrame(conversation_brief_outputs),
            metadata={
                "num_rows": str(len(conversation_brief_outputs)),
            },
        )
