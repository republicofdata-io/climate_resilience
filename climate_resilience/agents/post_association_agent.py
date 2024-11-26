from typing import List

from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langsmith import traceable
from pydantic import BaseModel, Field


# Define classes for LLM task output
class PostAssociation(BaseModel):
    """Association between post and narrative"""

    post_id: str = Field(description="A post's id")
    text: str = Field(description="A post's text")
    discourse: str = Field(description="The associated discourse's label")
    narrative: str = Field(
        description="A description of the discourse underlying the post, in the context of the climate change event being discussed"
    )


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
    You are an expert at analyzing discourses from social network posts.

    # STEPS
    1. Ingest a json object which has a post from a social network conversation discussing a climate event.
    2. Take into consideration the climate event summary as the context for that post.
    3. Consider the discourse type definitions provided below.
    4. Associate the most appropriate discourse type to this post.
    5. It's important that if no discourse is relevant, the post should be classified as N/A.
    6. Provide a narrative that describes the discourse underlying the post in the context of the climate change event being discussed.
    7. Each association should have the post's id, text, the discourse's label and narrative description.

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
                "Here's a json object which has a post from a social network conversation discussing a climate change event: {conversation_post_json}",
            ),
        ]
    ).partial(format_instructions=parser.get_format_instructions())

    # Task
    chain = prompt_template | model | parser
    return chain
