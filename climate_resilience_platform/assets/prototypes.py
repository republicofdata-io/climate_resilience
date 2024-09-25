from typing import TypedDict

import pandas as pd
from dagster import MetadataValue, Output, asset
from dagster_hex.resources import DEFAULT_POLL_INTERVAL
from dagster_hex.types import HexOutput

from ..resources.hex_resource import ConfigurableHexResource


class ConversationBrief(TypedDict):
    conversation_id: str


@asset(
    name="investigative_reporter_ai_agent",
    description="Investigative Reporter AI Agent",
    io_manager_key="bigquery_io_manager",
    group_name="prototypes",
    key_prefix=["prototypes"],
)
def investigative_reporter_ai_agent(hex_resource: ConfigurableHexResource):
    hex_client = hex_resource.create_client()

    hex_output: HexOutput = hex_client.run_and_poll(
        project_id="00c977d2-e2c7-43a0-abfc-3d466dbad3c1",
        inputs=None,
        kill_on_timeout=True,
        poll_interval=DEFAULT_POLL_INTERVAL,
        poll_timeout=None,
    )

    conversation_brief_output = ConversationBrief(conversation_id="1234567890")

    return Output(
        value=pd.DataFrame([conversation_brief_output]),
        metadata={
            "run_url": MetadataValue.url(hex_output.run_response["runUrl"]),
            "run_status_url": MetadataValue.url(
                hex_output.run_response["runStatusUrl"]
            ),
            "trace_id": MetadataValue.text(hex_output.run_response["traceId"]),
            "run_id": MetadataValue.text(hex_output.run_response["runId"]),
            "elapsed_time": MetadataValue.int(
                hex_output.status_response["elapsedTime"]
            ),
        },
    )
