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

        # Check for 404 or other errors
        if response.status_code == 404:
            print("Error 404: Profile not found.")
            return pd.DataFrame()  # Return an empty DataFrame

        if not response.ok:
            print(f"Error {response.status_code}: {response.text}")
            return pd.DataFrame()  # Return an empty DataFrame

        # Parse the JSON response
        response_json = response.json()

        # Extract geographical info
        geo_info = {
            "country": response_json.get("country"),
            "country_full_name": response_json.get("country_full_name"),
            "city": response_json.get("city"),
            "state": response_json.get("state"),
        }

        # Convert to DataFrame
        response_df = pd.DataFrame([geo_info])

        return response_df
