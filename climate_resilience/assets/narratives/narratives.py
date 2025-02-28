import json
import os
from datetime import datetime
from pydantic import BaseModel, Field

import pandas as pd
from dagster import AssetIn, Output, TimeWindowPartitionMapping, asset
from dagster_gcp import BigQueryResource
from google.api_core.exceptions import GoogleAPIError

from ...agents import conversation_classification_agent, post_association_agent
from ...partitions import three_hour_partition_def
from ...utils.conversations import assemble_conversations


class PostAssociation(BaseModel):
    """Association between post and discourse"""

    post_id: str = Field(description="A post's id")
    post_type: str = Field(description="Classification of the type of post")
    discourse_category: str = Field(description="The associated discourse category's label.")
    discourse_sub_category: str = Field(description="The associated discourse sub-category's label.")
    narrative: str = Field(description="A concise summary of the post's underlying perspective or storyline.")
    justification: str = Field(description="A detailed explanation of how the discourse category, sub-category, and narrative were determined, referencing key textual elements or rhetorical cues in the post.")
    confidence: float = Field(description="A confidence score (0-1) indicating the certainty of the discourse and narrative classification.")
    partition_time: datetime = Field(description="The time at which the post was classified.")


class ConversationClassification(BaseModel):
    """Classify if a conversation is about climate change"""

    conversation_id: str = Field(description="A conversation's id")
    classification: bool = Field(
        description="Whether the conversation is about climate change"
    )
    partition_time: datetime = Field(description="The time at which the conversation was classified.")


@asset(
    name="conversation_classifications",
    description="Classification of conversations as climate-related or not",
    io_manager_key="narratives_io_manager",
    ins={
        "x_conversations": AssetIn(
            key=["social_networks", "x_conversations"],
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-4, end_offset=-4
            ),
        ),
        "x_conversation_posts": AssetIn(
            key=["social_networks", "x_conversation_posts"],
            partition_mapping=TimeWindowPartitionMapping(start_offset=-4, end_offset=0),
        ),
    },
    partitions_def=three_hour_partition_def,
    metadata={"partition_expr": "partition_time"},
    output_required=False,
    compute_kind="LangChain",
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
    conversation_classifications = []

    if not x_conversations.empty:
        # Assemble full conversations
        conversations_df = assemble_conversations(
            context, 
            conversations=x_conversations, 
            posts=x_conversation_posts
        )

        # Group by tweet_conversation_id and aggregate tweet_texts into a list ordered by tweet_created_at
        conversations_df = (
            conversations_df.groupby("post_conversation_id")
            .apply(
                lambda x: x.sort_values("post_created_at")[
                    ["post_id", "post_created_at", "post_text"]
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
            conversation_json = json.dumps(conversation_dict, default=str)
            context.log.info(f"Classifying conversation: {conversation_json}")

            conversation_classifications_output = (
                conversation_classification_agent.invoke(
                    {"conversation_posts_json": conversation_json}
                )
            )

            conversation_classifications.append(
                ConversationClassification(
                    conversation_id=conversation_dict["post_conversation_id"],
                    classification=str(
                        conversation_classifications_output.dict()["classification"]
                    ),
                    partition_time=partition_time,
                )
            )

    if conversation_classifications:
        # Convert list of PostAssociation objects to list of dicts
        conversation_classifications_dicts = [assoc.dict() for assoc in conversation_classifications]

        # Convert to DataFrame
        conversation_classifications_df = pd.DataFrame(conversation_classifications_dicts)
        conversation_classifications_df['classification'] = conversation_classifications_df['classification'].astype(str)

        # Ensure column names are strings
        conversation_classifications_df.columns = conversation_classifications_df.columns.map(str)

        context.log.info(f"Final DataFrame before yielding: {conversation_classifications_df}")

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
    io_manager_key="narratives_io_manager",
    ins={
        "x_conversations": AssetIn(
            key=["social_networks", "x_conversations"],
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-4, end_offset=-4
            ),
        ),
        "x_conversation_posts": AssetIn(
            key=["social_networks", "x_conversation_posts"],
            partition_mapping=TimeWindowPartitionMapping(start_offset=-4, end_offset=0),
        ),
        "conversation_classifications": AssetIn(
            key=["narratives", "conversation_classifications"],
            partition_mapping=TimeWindowPartitionMapping(start_offset=0, end_offset=0),
        ),
        "conversation_event_summary": AssetIn(
            key=["narratives", "conversation_event_summary"],
        ),
        "articles": AssetIn(
            key=["media", "nytimes_articles"],
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-12, end_offset=0
            ),
        ),
    },
    partitions_def=three_hour_partition_def,
    metadata={"partition_expr": "partition_time"},
    output_required=False,
    compute_kind="LangChain",
)
def post_narrative_associations(
    context,
    x_conversations,
    x_conversation_posts,
    conversation_classifications,
    conversation_event_summary,
    articles,
    gcp_resource: BigQueryResource,
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
    context.log.info(
        f"Partition key range for conversation_event_summary: {context.asset_partition_key_range_for_input('conversation_event_summary')}"
    )
    context.log.info(
        f"Partition key range for articles: {context.asset_partition_key_range_for_input('articles')}"
    )

    # Get partition's time
    partition_time_str = context.partition_key
    partition_time = datetime.strptime(partition_time_str, "%Y-%m-%d-%H:%M")

    # Initialize DataFrame to store classifications
    post_associations = []

    if not x_conversations.empty:
        # Fetch all event summaries from the conversations in x_conversations
        sql = f"""
        select * from {os.getenv("BIGQUERY_PROJECT_ID")}.{os.getenv("BIGQUERY_NARRATIVES_DATASET")}.conversation_event_summary
        where conversation_id in ({','.join(map(lambda x: f"'{x}'", x_conversations["tweet_conversation_id"].to_list()))})
        """

        with gcp_resource.get_client() as client:
            job = client.query(sql)
            job.result()  # Wait for the job to complete

            if job.error_result:
                error_message = job.error_result.get("message", "Unknown error")
                raise GoogleAPIError(f"BigQuery job failed: {error_message}")
            else:
                event_summary_df = job.to_dataframe()

        context.log.info(
            f"Number of event summaries fetched: {event_summary_df.shape[0]}"
        )

        # Assemble full conversations
        conversations_df = assemble_conversations(
            context,
            conversations=x_conversations,
            posts=x_conversation_posts,
            classifications=conversation_classifications,
            event_summaries=event_summary_df,
            articles=articles,
        )
        context.log.info(
            f"Associating discourse type and extracting narrative for {len(conversations_df)} social network conversation posts."
        )

        # Iterate over all conversations and classify them
        for _, conversation_post in conversations_df.iterrows():
            try:
                post_association_output = post_association_agent.invoke(
                    {
                        "post_id": conversation_post["post_id"],
                        "event_summary": conversation_post["event_summary"],
                        "initial_post_text": conversation_post["initial_post_text"],
                        "post_text": conversation_post["post_text"],
                    }
                )
                associations_list = list(post_association_output.post_associations)

                for association in associations_list:
                    context.log.info(f"Processing association: {association}")

                    post_associations.append(
                        PostAssociation(
                            post_id=association.post_id,
                            post_type=association.post_type,
                            discourse_category=association.discourse_category,  # <-- Check if `category` exists
                            discourse_sub_category=association.discourse_sub_category,  # <-- Check if `sub_category` exists
                            narrative=association.narrative,
                            justification=association.justification,
                            confidence=association.confidence,
                            partition_time=partition_time,
                        )
                    )

            except Exception as e:
                context.log.error(f"Failed to associate posts")
                context.log.error(e)

    if post_associations:
        # Convert list of PostAssociation objects to list of dicts
        post_associations_dicts = [assoc.dict() for assoc in post_associations]

        # Convert to DataFrame
        post_associations_df = pd.DataFrame(post_associations_dicts)

        # Ensure column names are strings
        post_associations_df.columns = post_associations_df.columns.map(str)

        context.log.info(f"Final DataFrame before yielding: {post_associations_df}")

        # Return asset
        yield Output(
            value=post_associations_df,
            metadata={
                "num_rows": post_associations_df.shape[0],
            },
        )
