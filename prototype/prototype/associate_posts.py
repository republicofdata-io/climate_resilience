import json

import pandas as pd

from prototype.post_association_agent import initiate_post_association_agent
from prototype.utils import get_conversations

conversations_df = get_conversations()
classifications_df = pd.read_csv("data/conversation_classifications.csv")

# Filter conversations classified as about climate change
climate_change_conversations_df = conversations_df[
    conversations_df["conversation_id"].isin(
        classifications_df[classifications_df["classification"] == True][
            "conversation_id"
        ]
    )
]

# # Associate posts with a discourse type
post_associations_df = pd.DataFrame(columns=["post_id", "discourse_type"])
post_association_agent = initiate_post_association_agent()

# Iterate over all conversations and classify them
for _, conversation_df in climate_change_conversations_df.iterrows():
    conversation_dict = conversation_df.to_dict()
    conversation_json = json.dumps(conversation_dict)

    try:
        post_associations_output = post_association_agent.invoke(
            {"conversation_posts_json": conversation_json}
        )

        for association in post_associations_output.post_associations:
            new_row = {
                "post_id": association.post_id,
                "discourse_type": association.discourse,
            }
            post_associations_df = pd.concat(
                [post_associations_df, pd.DataFrame([new_row])], ignore_index=True
            )
    except Exception as e:
        print(
            f"Failed to associate posts in conversation {conversation_df['conversation_id']}"
        )
        print(e)

# Save classified conversations to a new csv file
post_associations_df.to_csv("data/post_associations_df.csv", index=False)
