import re

import pandas as pd


def assemble_conversations(
    context, conversations, posts, classifications=None, event_summary=None
):
    # Assemble full conversations
    assembled_conversations = (
        (
            pd.merge(
                conversations,
                posts,
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
    assembled_conversations["tweet_text"] = assembled_conversations["tweet_text"].apply(
        lambda x: re.sub(r"@\w+", "", x).strip()
    )

    # Filter conversations by classification if provided
    if classifications is not None:
        assembled_conversations = pd.merge(
            assembled_conversations,
            classifications,
            left_on="tweet_conversation_id",
            right_on="conversation_id",
        ).drop(columns=["conversation_id", "partition_time"])

        assembled_conversations = assembled_conversations[
            assembled_conversations["classification"].astype(bool)
        ]

    # Add event summary to conversations if provided
    if event_summary is not None:
        assembled_conversations = pd.merge(
            assembled_conversations,
            event_summary,
            left_on="tweet_conversation_id",
            right_on="conversation_id",
        )

    return assembled_conversations
