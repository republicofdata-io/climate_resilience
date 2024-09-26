import os

from dagster import ConfigurableResource
from dagster_hex.resources import HexResource
from pydantic import Field


class ConfigurableHexResource(ConfigurableResource):
    hex_api_key: str = Field(default_factory=lambda: os.environ.get("HEX_API_KEY"))

    def create_client(self) -> HexResource:
        return HexResource(api_key=self.hex_api_key)

    def run_and_poll(
        self,
        project_id: str,
        inputs: dict,
        update_cache: bool,
        kill_on_timeout: bool,
        poll_interval: int,
        poll_timeout: int,
    ):
        return self.hex_resource.run_and_poll(
            project_id=project_id,
            inputs=inputs,
            update_cache=update_cache,
            kill_on_timeout=kill_on_timeout,
            poll_interval=poll_interval,
            poll_timeout=poll_timeout,
        )
