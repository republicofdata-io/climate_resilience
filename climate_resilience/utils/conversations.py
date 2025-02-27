import re

import pandas as pd


def assemble_conversations(
    context, conversations, posts, classifications=None, event_summary=None, articles=None
):
    # Create base dataframe of all posts coming from both conversations and posts
    assembled_conversations = conversations[['tweet_id', 'tweet_conversation_id', 'tweet_created_at', 'tweet_text', 'article_url']]
    assembled_conversations = assembled_conversations.append(
        posts[['tweet_id', 'tweet_conversation_id', 'tweet_created_at', 'tweet_text', 'article_url']]
    )

    # Join conversations to that base dataframe
    assembled_conversations = pd.merge(
        assembled_conversations, conversations[['tweet_text']], how='left', on='tweet_conversation_id'
    )
    assembled_conversations = assembled_conversations.rename(
        columns={
            'post_id': 'tweet_id', 
            'post_conversation_id': 'tweet_conversation_id', 
            'post_created_at': 'tweet_created_at', 
            'post_text': 'tweet_text',
            'article_url': 'tweet_article_url',
            'initial_post_text': 'post_text_y'
        })

    # TODO: Join articles
    # TODO: Join event summaries
    # TODO: Coalesce event summaries and article summaries
    # TODO: Filter by classification if provided
    # TODO: Final selection of columns

    return assembled_conversations
