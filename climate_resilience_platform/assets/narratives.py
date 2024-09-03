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

    context.log.info(
        f"Classifying {len(social_network_x_conversations)} social network conversation posts."
    )

    return None
