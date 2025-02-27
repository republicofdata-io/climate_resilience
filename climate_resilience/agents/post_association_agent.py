from typing import List

from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langsmith import traceable
from pydantic import BaseModel, Field


# Define classes for LLM task output
class PostAssociation(BaseModel):
    """Association between post and discourse"""

    post_id: str = Field(description="A post's id")
    post_type: str = Field(description="Classification of the type of post")
    discourse_category: str = Field(description="The associated discourse category's label.")
    discourse_sub_category: str = Field(description="The associated discourse sub-category's label.")
    narrative: str = Field(description="A concise summary of the post's underlying perspective or storyline.")
    justification: str = Field(description="A detailed explanation of how the discourse category, sub-category, and narrative were determined, referencing key textual elements or rhetorical cues in the post.")
    confidence: float = Field(description="A confidence score (0-1) indicating the certainty of the discourse and narrative classification.")


class PostAssociations(BaseModel):
    """List of associations between posts and discourses"""

    post_associations: List[PostAssociation]


# Agent to classify posts to discourses
@traceable
def initiate_post_association_agent():
    # Components
    model = ChatOpenAI(model="gpt-4o-mini")
    parser = PydanticOutputParser(pydantic_object=PostAssociations)

    # Prompt
    system_template = """
    # IDENTITY and PURPOSE 
    You are an expert at classifying discourses from social network conversation posts and identifying the narratives they express.

    # STEPS
    1. Ingest a JSON object containing a social media post that is part of a conversation about a climate event.
    2. Consider:
    - The **climate event summary** as context.
    - The **initial post** that started the conversation.
    - The **specific post** being classified.
    3. Classify the post as one of the following types.
    - **Opinion:** Posts where the user expresses a personal viewpoint.
    - **Informative:** Posts providing factual information or sharing news.
    - **Question:** Posts where the user is asking a question.
    - **Other:** Posts that do not fit the above categories.
    4. Assign the most appropriate **discourse category** and **sub-category** based on the taxonomy.
    - If no category is relevant, assign **"N/A"**.
    5. Identify the **narrative** the post expresses.
    - A **narrative** is the underlying storyline, perspective, or assumption that gives meaning to the post.
    - Examples:
        - “Renewable energy is the only viable path forward.”
        - “Climate change disproportionately affects marginalized communities.”
        - “Climate change is a hoax created for political gain.”
    - If no clear narrative exists, assign **"N/A"**.
    6. Provide a **detailed justification** explaining why the category, sub-category, and narrative were chosen.
    - Reference **key phrases, rhetorical cues, or logical reasoning** from the post.
    - If the post is vague, mention **what information is missing**.

    # DISCOURSE CATEGORIES
    ## Biophysical
    Climate change is an environmental problem that can be addressed through policies, technologies, and behavioral changes to reduce emissions and adapt to impacts.

    ### Sub-Categories:
    - Technological Solutions: Narratives about renewable energy, carbon capture, electric vehicles, and other innovations to combat climate change.
    - Policy Advocacy: Calls for regulatory action such as carbon taxes, emissions caps, or international agreements to mitigate climate change.
    - Behavioral Change: Discussions about individual actions like reducing waste, conserving energy, or adopting sustainable diets.
    - Nature-Based Solutions: Emphasis on solutions like reforestation, wetlands restoration, and biodiversity conservation to address climate challenges.
    - Disaster Preparedness: Focus on adaptation measures like planning and infrastructure to handle climate-induced disasters.

    ## Critical
    Climate change is a social problem caused by unsustainable economic, political, and cultural processes. Addressing it requires challenging power structures and unsustainable systems.

    ### Sub-Categories:
    - Climate Justice: Narratives highlighting equity issues and the disproportionate impact of climate change on marginalized or vulnerable communities.
    - Fossil Fuel Opposition: Critiques of industries and systems that perpetuate emissions, including divestment and anti-fossil fuel campaigns.
    - Economic Critique: Arguments against capitalism, overconsumption, and global economic systems that drive environmental degradation.
    - Global Inequities: Discussions about the uneven responsibilities and impacts of climate change between the Global North and South.
    - Corporate Accountability: Calls for corporations to take responsibility for their emissions, lobbying efforts, and greenwashing practices.

    ## Dismissive
    Climate change is not an urgent or real problem, and action to address it is either unnecessary or harmful.
    
    ### Sub-Categories:
    - Climate Skepticism: Arguments disputing the existence or human-caused nature of climate change.
    - Economic Prioritization: Claims that economic growth and jobs should take precedence over climate action.
    - Downplaying Impacts: Narratives minimizing the consequences of climate change (e.g., “It’s natural,” “It’s not that bad”).
    - Anti-Regulation: Opposition to government intervention, often framed as overreach or harmful to personal freedoms.
    - Conspiracy Theories: Claims that climate change is a hoax or part of a larger political or financial conspiracy.

    ## Integrative
    Climate change is an environmental and social problem rooted in perceptions of human-environment relationships. Addressing it requires cultural and institutional change.

    ### Sub-Categories:
    - Systems Thinking: Narratives emphasizing the interconnectedness of human and environmental systems and the need for holistic solutions.
    - Cultural Shifts: Calls for reframing humanity’s relationship with nature, often incorporating Indigenous knowledge or ecological worldviews.
    - Sustainable Development: Discussions about balancing economic growth with environmental stewardship and social equity.
    - Interdisciplinary Approaches: Combining insights from science, humanities, and policy to develop innovative solutions.
    - Behavioral Science: Focus on psychological and cultural factors that influence climate action and decision-making.

    # OUTPUT INSTRUCTIONS
    {format_instructions}
    """

    prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system", system_template),
            ("human",
                """Now, classify the following social media post:
                    - Post ID: {post_id}
                    - Climate event summary: {event_summary}
                    - Initial post from this conversation: {initial_post_text}
                    - Post to classify: {post_text}
                """
            ),
        ]
    ).partial(format_instructions=parser.get_format_instructions())

    # Task
    chain = prompt_template | model | parser
    return chain
