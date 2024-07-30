import json
import re
from typing import List

import pandas as pd
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.pydantic_v1 import BaseModel, Field
from langchain_openai import ChatOpenAI
from langsmith import traceable


# Define classes for LLM task output
class Narrative(BaseModel):
    """Information about a narrative."""

    label: str = Field(
        description="A representative label to identify the identified narrative"
    )
    description: str = Field(description="A short description of the narrative")


class Narratives(BaseModel):
    """Identifying information about all narratives in a conversation."""

    narratives: List[Narrative]


def remove_user_mentions(text):
    return re.sub(r"@\w+", "", text).strip()


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


@traceable
def initiate_narratives_agent():
    # Components
    model = ChatOpenAI(model="gpt-4")
    parser = PydanticOutputParser(pydantic_object=Narratives)

    # Prompt
    system_template = """
    # IDENTITY and PURPOSE 
    You are an expert at extracting narratives from conversations.

    # STEPS
    1. Ingest the json file which has conversations on climate change
    2. Take your time to process all its entries
    3. Parse all conversations and extract all narratives
    4. Each narrative should have a label and a short description (5 to 10 words)

    # OUTPUT INSTRUCTIONS
    {format_instructions}
    """

    prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system", system_template),
            ("human", "{text}"),
        ]
    ).partial(format_instructions=parser.get_format_instructions())

    # Task
    chain = prompt_template | model | parser
    return chain


def get_narratives(agent, conversation):
    conversation_dict = conversation.to_dict()
    conversation_json = json.dumps(conversation_dict)
    output = agent.invoke({"text": conversation_json})

    # Convert the Narratives object to a dictionary
    narratives_dict = output.dict()

    return narratives_dict


def main():
    conversations_df = get_conversations()
    narratives_agent = initiate_narratives_agent()

    # Print number of posts in the first conversation
    print(len(conversations_df.iloc[0]["posts"]))

    # TODO: Refactor AI agent to classify based on existing 4 discourses. Important that agent should only classify if confident enough about classification.
    # TODO: Loop through all conversations and classify posts
    # TODO: Save classified posts to a new csv file
    narratives_json = get_narratives(narratives_agent, conversations_df.iloc[0])
    print(narratives_json)


if __name__ == "__main__":
    main()
