from typing import List

from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.pydantic_v1 import BaseModel, Field
from langchain_openai import ChatOpenAI
from langsmith import traceable


# Define classes for LLM task output
class PostAssociation(BaseModel):
    """Association between post and narrative"""

    post_id: str = Field(description="A post's id")
    text: str = Field(description="A post's text")
    discourse: str = Field(description="The associated discourse's label")


class PostAssociations(BaseModel):
    """List of associations between posts and narratives"""

    post_associations: List[PostAssociation]


# Agent to classify posts to discourses
@traceable
def initiate_post_association_agent():
    # Components
    model = ChatOpenAI(model="gpt-4o")
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
