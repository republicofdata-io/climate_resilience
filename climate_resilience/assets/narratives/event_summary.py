from datetime import datetime

import pandas as pd
from dagster import AssetIn, Output, TimeWindowPartitionMapping, asset
from typing_extensions import Optional, TypedDict

from ...agents import conversation_event_summary_agent
from ...partitions import three_hour_partition_def


class Conversation(TypedDict):
    id: str
    conversation: str


class Article(TypedDict):
    url: Optional[str]
    title: Optional[str]
    summary: Optional[str]


class EventSummary(TypedDict):
    conversation_id: str
    event_summary: str
    research_cycles: int
    partition_time: datetime


@asset(
    name="conversation_event_summary",
    description="Summary of the event discussed in a conversation",
    io_manager_key="narratives_io_manager",
    ins={
        "articles": AssetIn(
            key=["media", "nytimes_articles"],
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-12, end_offset=-4
            ),
        ),
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
def conversation_event_summary(
    context,
    articles,
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
    event_summaries = []

    if not x_conversations.empty:
        for _, conversation in x_conversations.iterrows():
            # Conversation's tweet text
            conversation_list = [conversation["tweet_text"]]

            for _, post in x_conversation_posts.iterrows():
                if (
                    post["tweet_conversation_id"]
                    == conversation["tweet_conversation_id"]
                ):
                    conversation_list.append(post["tweet_text"])

            context.log.info(
                f"Number of posts for conversation {conversation['tweet_conversation_id']}: {len(conversation_list)}"
            )

            if len(conversation_list) >= 2:
                context.log.info(
                    f"Launching Investigative Reporter AI Agent for conversation {conversation['tweet_conversation_id']}"
                )

                # Package conversation into a markdown string
                conversation_markdown = f"# {conversation['tweet_conversation_id']}\n\n"
                conversation_markdown += "\n".join(
                    [f"- {tweet}" for tweet in conversation_list]
                )
                context.log.info(f"Conversation: {conversation_markdown}")

                # Append article's title and summary to the conversation
                article_url = None
                article_title = None
                article_summary = None

                for _, article in articles.iterrows():
                    if article["link"] == conversation["article_url"]:
                        article_url = article["link"]
                        article_title = article["title"]
                        article_summary = article["summary"]
                        break

                conversation = Conversation(
                    id=conversation["tweet_conversation_id"],
                    conversation=conversation_markdown,
                )

                article = Article(
                    url=article_url,
                    title=article_title,
                    summary=article_summary,
                )

                conversation_event_summary_output = (
                    conversation_event_summary_agent.invoke(
                        {
                            "conversation": conversation,
                            "article": article,
                            "completeness_assessment": False,
                            "research_cycles": 0,
                        }
                    )
                )
                event_summary = EventSummary(
                    conversation_id=conversation["id"],
                    event_summary=conversation_event_summary_output["event_summary"],
                    research_cycles=conversation_event_summary_output[
                        "research_cycles"
                    ],
                    partition_time=partition_time,
                )

                event_summaries.append(event_summary)

    if event_summaries:
        yield Output(
            value=pd.DataFrame(event_summaries),
            metadata={
                "num_rows": str(len(event_summaries)),
            },
        )
