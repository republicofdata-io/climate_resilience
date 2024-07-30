import json
import re
from typing import List

import pandas as pd
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.pydantic_v1 import BaseModel, Field
from langchain_ollama.llms import OllamaLLM
from langsmith import traceable


# Define classes for LLM task output
class PostAssociation(BaseModel):
    """Association between post and narrative"""

    text: str = Field(description="A post's text")
    discourse: str = Field(description="The associated discourse's label")


class PostAssociations(BaseModel):
    """List of associations between posts and narratives"""

    post_associations: List[PostAssociation]


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


# Agent to classify posts to discourses
@traceable
def initiate_discourse_association_agent():
    # Components
    model = OllamaLLM(model="llama3.1")
    parser = PydanticOutputParser(pydantic_object=PostAssociations)

    # Prompt
    system_template = """
    # IDENTITY and PURPOSE 
    You are an expert at associating discourse types to social network posts.

    # STEPS
    1. Ingest the first json object which has all the posts from a social network conversation on climate change.
    2. Consider the discourse type definitions provided below.
    3. Take your time to process all those entries.
    4. Parse all posts and associate the most appropriate discourse type to each individual post.
    5. It's important that if no discourse is relevant, the post should be classified as N/A.
    5. Each association should have the post's text and the discourse's label.

    # DISCOURSE TYPES
    1. Biophysical: "Climate change is an environmental problem caused by rising concentrations of greenhouse gases from human activities. Climate change can be addressed through policies, technologies, and behavioural changes that reduce greehouse gas emissions and support adaptation."
    2. Critical: "Climate change is a social problem caused by economic, political, and cultureal procsses that contribute to uneven and unsustainable patterns of development and energy usage. Addressing climate change requires challenging economic systems and power structures that perpetuate high levels of fossil fuel consumption."
    3. Dismissive: "Climate change is not a problem at all or at least not an urgent concern. No action is needed to address climate change, and other issues should be prioritized."
    4. Integrative: "Climate change is an environmental and social problem that is rooted in particular beliefs and perceptions of human-environment relationships and humanity's place in the world. Addressing climate change requires challenging mindsets, norms, rules, institutions, and policies that support unsustainable resource use and practice."
    5. N/A: "No discourse is relevant to this post."

    # OUTPUT INSTRUCTIONS
    {format_instructions}
    """

    prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system", system_template),
            (
                "human",
                "Here's a json object which has all the posts from a social network conversation on climate change: {conversation_posts_json}",
            ),
        ]
    ).partial(format_instructions=parser.get_format_instructions())

    # Task
    chain = prompt_template | model | parser
    return chain


# Invoke agent to associate posts to discourses
def associate_discourses_to_posts(agent, conversation):
    conversation_dict = conversation.to_dict()
    conversation_json = json.dumps(conversation_dict)
    output = agent.invoke({"conversation_posts_json": conversation_json})
    post_discourse_associations = output.dict()

    return post_discourse_associations


# TODO: Use local Llama 3.1 to classify posts
# TODO: Identify conversations that are about climate change
# TODO: Loop through all conversations and classify posts
# TODO: Save classified posts to a new csv file
def main():
    conversations_df = get_conversations()
    agent = initiate_discourse_association_agent()

    # Print number of posts in the first conversation
    print(len(conversations_df.iloc[0]["posts"]))

    post_discourse_associations_json = associate_discourses_to_posts(
        agent, conversations_df.iloc[0]
    )
    print(post_discourse_associations_json)


if __name__ == "__main__":
    main()
