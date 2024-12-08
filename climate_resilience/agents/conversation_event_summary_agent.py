from operator import add
from typing import Annotated, Any, Dict, List, Literal

from langchain_community.tools import DuckDuckGoSearchRun, WikipediaQueryRun
from langchain_community.utilities import WikipediaAPIWrapper
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, Field
from typing_extensions import TypedDict

model = ChatOpenAI(model="gpt-4o-mini")


# LLM Chain - Notepad Completeness Assessment
class Assessment(BaseModel):
    completeness_assessment: bool = Field(
        description="Assessment on whether to accumulated information in the notepad is sufficient for a meaningful event summary."
    )
    suggested_investigations: list[str] = Field(
        description="Additional research tasks suggested to be able to generate a meaningful event summary"
    )


completeness_assessment_structured_model = model.with_structured_output(Assessment)

completeness_assessment_prompt_template = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """You are an expert evaluator tasked with assessing the sufficiency of the information accumulated in the notepad for generating a meaningful and informative summary about an event. The notepad contains an article, a conversation about this article, and research findings.

                Please analyze the notepad and determine:

                    •	Sufficiency: Is there enough comprehensive and coherent information to draft a clear and informative summary of the event?
                    •	Gaps: Are there any missing elements or gaps that require additional research?

                Provide a concise assessment indicating whether the information is sufficient. If it is not sufficient, specify the areas that need further investigation.”            """,
        ),
        (
            "human",
            """Please provide an assessment of whether the following research findings are enough to generate a meaningful and informative summary of the event discussed in this conversation
            
            Article url: {article_url}

            Article title: {article_title}

            Article summary: {article_summary}
            
            Conversation: {conversation}
            
            Resarch findings: {research_findings}
            
            Avoid duplicating the tasks and only suggest new areas for investigation if necessary.""",
        ),
    ]
)

completeness_assessment_llm_chain = (
    completeness_assessment_prompt_template | completeness_assessment_structured_model
)


## LLM Chain - Tool Choice
class ToolChoice(BaseModel):
    tool_choice: str = Field(
        description="Tool selection between the following choices: 'Wikipedia', 'DuckDuckGo'"
    )


tool_choice_structured_model = model.with_structured_output(ToolChoice)

tool_choice_prompt_template = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """You are an expert tasked with selecting the most appropriate tool to perform a research task.

            You have the following tools available:
            - **Wikipedia**: Best for looking up established, well-documented facts, historical data, background on general knowledge, and publicly known information.
            - **DuckDuckGo**: Best for up-to-date web searches, news, real-time information, and information that may not be widely documented in an encyclopedia.
            
            Choose the most appropriate tool based on the nature of the research task provided.
            """,
        ),
        (
            "human",
            """Research Task: {research_task}
            
            Based on the nature of this task, please choose between 'Wikipedia' and 'DuckDuckGo' as the tool that will provide the most relevant and useful information.
            """,
        ),
    ]
)

tool_choice_llm_chain = tool_choice_prompt_template | tool_choice_structured_model


## LLM Chain - Findings Summary
class FindingsSummary(BaseModel):
    summary: str = Field(description="A summary of the findings for the research task.")


findings_summary_structured_model = model.with_structured_output(FindingsSummary)

findings_summary_prompt_template = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """You are an expert tasked with generating a meaningful and informative summary of your findings for a research task you were given. Ensure that the summary is clear, informative, and answers the research task.
            """,
        ),
        (
            "human",
            """Please provide a summary for the following research findings:
            
            Research task: {research_task}
            
            Research findings: {research_findings}
            
            Summarize the key takeaways.""",
        ),
    ]
)

findings_summary_llm_chain = (
    findings_summary_prompt_template | findings_summary_structured_model
)


## LLM Chain - Event Summary Generation
class EventSummary(BaseModel):
    event_summary: str = Field(
        description="A concise summary of the event being discussed in the conversation."
    )


event_summary_structured_model = model.with_structured_output(EventSummary)

event_summary_prompt_template = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """You are an expert tasked with generating a concise event summaries based on an article, a conversation about this article and accumulated research findings.
            
            The goal is to produce a clear description of the event covered by the article, disccused in the conversation and summarize the most important findings.
            
            - The event summary should be **no longer than 2-3 paragraphs**.
            - Do **not** include opinions or recommendations.
            - Do **not** use bullet points or lists.
            - Focus on describing the **essence of the event** and the key findings from the research in simple, cohesive text.""",
        ),
        (
            "human",
            """Please write a concise event summary that summarizes the following article, conversation and research findings.
            
            Article url: {article_url}

            Article title: {article_title}

            Article summary: {article_summary}
            
            Conversation: {conversation}
            
            Resarch findings: {research_findings}
            
            Ensure the event summary is in 2-3 paragraphs, avoids opinions or actions, and sticks to describing the event covered by the article, discussed in the conversation and the research findings.""",
        ),
    ]
)

event_summary_generation_llm_chain = (
    event_summary_prompt_template | event_summary_structured_model
)


## Agent's State
class Conversation(TypedDict):
    id: str
    conversation: str


class Article(TypedDict):
    url: str
    title: str
    summary: str


class ResearchFindings(TypedDict):
    task: str
    completed: bool
    tool_used: str
    findings: Annotated[list[str], add]
    summary: str


class ResearchState(TypedDict):
    conversation: Conversation
    article: Article
    completeness_assessment: bool
    research_cycles: int
    research_findings: Annotated[list[ResearchFindings], add]
    event_summary: str


## Tools
wikipedia = WikipediaQueryRun(api_wrapper=WikipediaAPIWrapper())
duckduckgo = DuckDuckGoSearchRun()


## Assessment Node
def assessment_node(state: ResearchState) -> ResearchState:
    print("---Assessment---")

    # Prepare input values
    research_findings_formatted = "\n".join(
        [f"{i+1}. {rf['task']}" for i, rf in enumerate(state["research_findings"])]
    )

    # Invoke the LLM chain
    output = completeness_assessment_llm_chain.invoke(
        {
            "article_url": state["article"]["url"],
            "article_title": state["article"]["title"],
            "article_summary": state["article"]["summary"],
            "conversation": state["conversation"]["conversation"],
            "research_findings": research_findings_formatted,
        }
    )

    # Extract output elements
    completeness_assessment = output.completeness_assessment
    suggested_investigations = output.suggested_investigations

    # Save output elements to agent's state
    for investigation in suggested_investigations:
        new_research_finding = {
            "task": investigation,
            "completed": False,
            "tool_used": "",
            "findings": [],  # Initially, findings are empty
        }
        state["research_findings"].append(new_research_finding)

    print("Assessment:", completeness_assessment)
    return {"completeness_assessment": completeness_assessment}


## Research Node
def research_node(state: ResearchState) -> ResearchState:
    print("---Research---")

    # Iterate over research findings and update incomplete tasks
    for research_finding in state["research_findings"]:
        if not research_finding["completed"]:  # Check if the task is incomplete
            # Decide which investigative tool to use
            tool_choice = tool_choice_llm_chain.invoke(research_finding["task"])
            research_finding["tool_used"] = tool_choice

            if tool_choice == "DuckDuckGo":
                result = duckduckgo.invoke(research_finding["task"])
            else:
                result = wikipedia.run(research_finding["task"])

            # Append result to the findings
            research_finding["findings"].append(result or "No result found")

            # Mark the task as completed
            research_finding["completed"] = True

            findings_summary = findings_summary_llm_chain.invoke(
                {
                    "research_task": research_finding["task"],
                    "research_findings": research_finding["findings"],
                }
            )

            research_finding["summary"] = findings_summary.summary

    print("Research Cycles:", state["research_cycles"] + 1)
    return {"research_cycles": state["research_cycles"] + 1}


## Event Summary Writing Node
def format_research_findings(research_findings: List[Dict[str, Any]]) -> str:
    """
    Formats the research findings into a structured string for the LLM prompt.

    Args:
        research_findings (List[Dict[str, Any]]): List of research findings with tasks, completed status, and findings.

    Returns:
        str: A formatted string that summarizes all the research findings.
    """
    formatted_findings = []

    for i, finding in enumerate(research_findings, 1):
        task = finding["task"]
        completed = "Completed" if finding["completed"] else "Incomplete"
        tool_used = finding["tool_used"] if finding["tool_used"] else "No tool used"
        summary = finding["summary"] if finding["summary"] else "No findings"

        # Formatting each research finding entry
        formatted_findings.append(
            f"Research Task {i}:\n"
            f"  Task: {task}\n"
            f"  Status: {completed}\n"
            f"  Tool used: {tool_used}\n"
            f"  Findings: {summary}"
        )

    # Join all the formatted findings into a single string
    return "\n\n".join(formatted_findings)


def write_event_summary_node(state: ResearchState) -> ResearchState:
    print("---Write event summary---")

    research_findings_formatted = format_research_findings(state["research_findings"])

    output = event_summary_generation_llm_chain.invoke(
        {
            "article_url": state["article"]["url"],
            "article_title": state["article"]["title"],
            "article_summary": state["article"]["summary"],
            "conversation": state["conversation"]["conversation"],
            "research_findings": research_findings_formatted,
        }
    )

    # The output will have the following structure:
    event_summary = output.event_summary

    print("Event Summary:", event_summary)
    return {"event_summary": event_summary}


## Edges
def assess_notepad_completeness(state) -> Literal["research", "write_event_summary"]:
    if state["completeness_assessment"] == True:
        return "write_event_summary"

    else:
        return "research"


def assess_research_limit(state) -> Literal["assessment", "write_event_summary"]:
    if state["research_cycles"] >= 3:
        return "write_event_summary"

    else:
        return "assessment"


def initiate_conversation_event_summary_agent():
    ## Agent
    # Build graph
    builder = StateGraph(ResearchState)
    builder.add_node("assessment", assessment_node)
    builder.add_node("research", research_node)
    builder.add_node("write_event_summary", write_event_summary_node)

    # Logic
    builder.add_edge(START, "assessment")
    builder.add_conditional_edges("assessment", assess_notepad_completeness)
    builder.add_conditional_edges("research", assess_research_limit)
    builder.add_edge("write_event_summary", END)

    # Add
    agent = builder.compile()

    return agent
