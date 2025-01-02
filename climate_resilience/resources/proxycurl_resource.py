import pandas as pd
import requests
from dagster import ConfigurableResource


class ProxycurlResource(ConfigurableResource):
    api_key: str

    def get_person_profile(self, x_profile_url):
        headers = {"Authorization": "Bearer " + self.api_key}
        api_endpoint = "https://nubela.co/proxycurl/api/v2/linkedin"
        params = {
            "twitter_profile_url": x_profile_url,
        }
        response = requests.get(api_endpoint, params=params, headers=headers)

        response_df = pd.DataFrame(response.data)

        return response_df
