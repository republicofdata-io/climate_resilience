import json

from prototype.agents import (
    initiate_conversation_classification_agent,
    initiate_post_association_agent,
)
from prototype.utils import get_conversations

# TODO: Identify conversations that are about climate change
# TODO: Loop through all conversations and classify posts
# TODO: Save classified posts to a new csv file
conversations_df = get_conversations().iloc[0]
conversations_dict = conversations_df.to_dict()
conversation_json = json.dumps(conversations_dict)

conversation_classification_agent = initiate_conversation_classification_agent()
conversation_classifications_output = conversation_classification_agent.invoke(
    {"conversation_posts_json": conversation_json}
)

print(conversation_classifications_output)
