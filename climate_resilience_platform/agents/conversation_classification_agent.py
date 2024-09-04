from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.pydantic_v1 import BaseModel, Field
from langchain_openai import ChatOpenAI
from langsmith import traceable


class ConversationClassification(BaseModel):
    """Classify if a conversation is about climate change"""

    conversation_id: str = Field(description="A conversation's id")
    classification: bool = Field(
        description="Whether the conversation is about climate change"
    )


# Agent to classify conversations as about climate change or not
@traceable
def initiate_conversation_classification_agent():
    # Components
    model = ChatOpenAI(model="gpt-4o-mini")
    structured_model = model.with_structured_output(ConversationClassification)

    prompt_template = ChatPromptTemplate.from_messages(
        [
            (
                "human",
                "Classify whether  this conversation is about climate change or not: {conversation_posts_json}",
            ),
        ]
    )

    # Task
    chain = prompt_template | structured_model
    return chain
