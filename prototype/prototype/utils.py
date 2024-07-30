import re

import pandas as pd


# Conversation cleanup util
def remove_user_mentions(text):
    return re.sub(r"@\w+", "", text).strip()


# Get data and structure conversations
def get_conversations():
    # Load data
    conversations_df = pd.read_csv("data/climate_conversations.csv")

    # Sort conversations
    conversations_sorted_df = conversations_df.sort_values(by="post_creation_ts")[
        ["conversation_id", "post_id", "post_creation_ts", "post_text"]
    ]

    # Remove user mentions from post_text
    conversations_sorted_df["post_text"] = conversations_sorted_df["post_text"].apply(
        remove_user_mentions
    )

    # Group by conversation_natural_key and aggregate post_texts into a list ordered by post_creation_ts
    conversations_sorted_df = (
        conversations_sorted_df.groupby("conversation_id")
        .apply(
            lambda x: x.sort_values("post_creation_ts")[
                ["post_id", "post_creation_ts", "post_text"]
            ].to_dict(orient="records")
        )
        .reset_index(name="posts")
    )

    return conversations_sorted_df
