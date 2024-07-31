import json

import pandas as pd

from prototype.conversation_classification_agent import (
    initiate_conversation_classification_agent,
)
from prototype.utils import get_conversations

conversations_df = get_conversations()

# Classify conversations as about climate change or not
conversation_classifications_df = pd.DataFrame(
    columns=["conversation_id", "classification"]
)
conversation_classification_agent = initiate_conversation_classification_agent()

# Iterate over all conversations and classify them
for _, conversation_df in conversations_df.iterrows():
    conversation_dict = conversation_df.to_dict()
    conversation_json = json.dumps(conversation_dict)

    conversation_classifications_output = conversation_classification_agent.invoke(
        {"conversation_posts_json": conversation_json}
    )
    new_classification = pd.DataFrame([conversation_classifications_output.dict()])
    conversation_classifications_df = pd.concat(
        [conversation_classifications_df, new_classification], ignore_index=True
    )

# Save classified conversations to a new csv file
conversation_classifications_df.to_csv(
    "data/conversation_classifications.csv", index=False
)
