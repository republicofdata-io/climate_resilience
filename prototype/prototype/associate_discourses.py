import json

from prototype.agents import (
    initiate_climate_change_classification_agent,
    initiate_discourse_association_agent,
)
from prototype.utils import get_conversations


# Invoke agent to associate posts to discourses
def classify_conversations(agent, conversation):
    conversation_dict = conversation.to_dict()
    conversation_json = json.dumps(conversation_dict)
    output = agent.invoke({"conversation_posts_json": conversation_json})
    conversation_classifications = output.dict()

    return conversation_classifications


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
conversations_df = get_conversations()
climate_change_classification_agent = initiate_climate_change_classification_agent()
discourse_association_agent = initiate_discourse_association_agent()

climate_change_conversations_json = classify_conversations(
    climate_change_classification_agent, conversations_df.iloc[0]
)
print(climate_change_conversations_json)

""" post_discourse_associations_json = associate_discourses_to_posts(
    agent, conversations_df.iloc[0]
)
print(post_discourse_associations_json) """
