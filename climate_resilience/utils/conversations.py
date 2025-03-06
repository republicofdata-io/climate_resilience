import pandas as pd


def assemble_conversations(
    context,
    conversations,
    posts,
    classifications=None,
    event_summaries=None,
    articles=None,
):
    # Create base dataframe of all posts coming from both conversations and posts
    assembled_conversations = conversations[
        [
            "tweet_id",
            "tweet_conversation_id",
            "tweet_created_at",
            "tweet_text",
            "article_url",
        ]
    ]
    assembled_conversations = pd.concat(
        [
            assembled_conversations,
            posts[
                [
                    "tweet_id",
                    "tweet_conversation_id",
                    "tweet_created_at",
                    "tweet_text",
                    "article_url",
                ]
            ],
        ],
        ignore_index=True,
    )

    # Join conversations to that base dataframe
    assembled_conversations = pd.merge(
        assembled_conversations,
        conversations[["tweet_conversation_id", "tweet_text"]],
        how="left",
        on="tweet_conversation_id",
    )
    assembled_conversations = assembled_conversations.rename(
        columns={
            "tweet_id": "post_id",
            "tweet_conversation_id": "post_conversation_id",
            "tweet_created_at": "post_created_at",
            "tweet_text_x": "post_text",
            "tweet_article_url": "article_url",
            "tweet_text_y": "initial_post_text",
        }
    )

    # Join articles
    if articles is not None:
        articles = articles.rename(
            columns={"link": "article_url", "summary": "article_summary"}
        )
        assembled_conversations = pd.merge(
            assembled_conversations,
            articles[["article_url", "article_summary"]],
            how="left",
            on="article_url",
        )

    # Join event summaries
    if event_summaries is not None:
        event_summaries = event_summaries.rename(
            columns={
                "CONVERSATION_ID": "post_conversation_id",
                "EVENT_SUMMARY": "event_summary",
            }
        )
        assembled_conversations = pd.merge(
            assembled_conversations,
            event_summaries[["post_conversation_id", "event_summary"]],
            how="left",
            on="post_conversation_id",
        )

    # Coalesce event summaries and article summaries
    if "event_summary" in assembled_conversations.columns:
        assembled_conversations["event_summary"] = assembled_conversations[
            "event_summary"
        ].combine_first(assembled_conversations["article_summary"])
    else:
        if "article_summary" in assembled_conversations.columns:
            assembled_conversations["event_summary"] = assembled_conversations[
                "article_summary"
            ]
        else:
            assembled_conversations["event_summary"] = None

    # Filter by classifcation
    if classifications is not None:
        classifications = classifications.rename(
            columns={"conversation_id": "post_conversation_id"}
        )
        assembled_conversations = pd.merge(
            assembled_conversations,
            classifications[["post_conversation_id", "classification"]],
            how="left",
            on="post_conversation_id",
        )
        assembled_conversations = assembled_conversations[
            assembled_conversations["classification"] == "True"
        ]

    # Final selection of columns
    assembled_conversations = assembled_conversations[
        [
            "post_id",
            "post_conversation_id",
            "post_created_at",
            "post_text",
            "initial_post_text",
            "event_summary",
        ]
    ]

    return assembled_conversations
