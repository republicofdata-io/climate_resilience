from typing import List

import pandas as pd
import requests
from dagster import ConfigurableResource


class XResourceException(Exception):
    def __init__(self, status_code, message):
        super().__init__(f"Status Code: {status_code}, Message: {message}")
        self.status_code = status_code


class XResource(ConfigurableResource):
    x_bearer_token: str

    def bearer_oauth(self, r):
        """
        Method required by bearer token authentication.
        """

        r.headers["Authorization"] = f"Bearer {self.x_bearer_token}"
        r.headers["User-Agent"] = "v2RecentSearchPython"
        return r

    def search(
        self, search_term: str, start_time: str, end_time: str, n_results: int = 10
    ) -> pd.DataFrame:
        """
        Get recent social_network_x_posts based on a search term.

        Parameters:
        - search_term: The search term to use for the X API query.
        - start_date: The start date for the X API query.
        - end_date: The end date for the X API query.
        - n_results: The maximum number of results to return (default is 10).

        Returns:
        A list of recent social_network_x_posts related to the search term.
        """

        query_params = {
            "query": f"{search_term}",
            "start_time": start_time,
            "end_time": end_time,
            "max_results": n_results,
            "sort_order": "recency",
            "tweet.fields": "created_at,conversation_id,author_id,text,public_metrics",
            "expansions": "author_id",
            "user.fields": "id,name,username,description,location,created_at,public_metrics",
        }

        response = requests.get(
            "https://api.twitter.com/2/tweets/search/recent",
            auth=self.bearer_oauth,
            params=query_params,
        )

        if response.status_code != 200:
            raise XResourceException(response.status_code, response.text)

        json_response = response.json()
        return self._parse_response_to_dataframe(json_response)

    def get_tweets(self, tweet_ids: str) -> pd.DataFrame:
        """
        Returns a variety of information about the Tweet specified by the requested list of IDs.

        Parameters:
        - tweet_ids: A comma separated list of Tweet IDs. Up to 100 are allowed in a single request. Make sure to not include a space between commas and fields.

        Returns:
        A list of recent social_network_x_posts related to the search term.
        """

        query_params = {
            "ids": f"{tweet_ids}",
            "tweet.fields": "created_at,conversation_id,author_id,text,public_metrics",
            "expansions": "author_id",
            "user.fields": "id,name,username,description,created_at,public_metrics",
        }

        response = requests.get(
            "https://api.twitter.com/2/tweets",
            auth=self.bearer_oauth,
            params=query_params,
        )

        if response.status_code != 200:
            raise XResourceException(response.status_code, response.text)

        json_response = response.json()
        return self._parse_response_to_dataframe(json_response)

    def _parse_response_to_dataframe(self, json_response) -> pd.DataFrame:
        data = json_response.get("data", [])
        includes = json_response.get("includes", {}).get("users", [])

        # Create a dictionary for user data for easy lookup
        users_dict = {user["id"]: user for user in includes}

        # Extract relevant data and construct a list of dictionaries
        rows = []
        for tweet in data:
            user = users_dict.get(tweet["author_id"], {})
            row = {
                "tweet_id": tweet["id"],
                "tweet_created_at": tweet["created_at"],
                "tweet_conversation_id": tweet["conversation_id"],
                "tweet_text": tweet["text"],
                "tweet_public_metrics": tweet["public_metrics"],
                "author_id": tweet["author_id"],
                "author_username": user.get("username", ""),
                "author_location": user.get("location", ""),
                "author_description": user.get("description", ""),
                "author_created_at": user.get("created_at", ""),
                "author_public_metrics": user.get("public_metrics", {}),
            }
            rows.append(row)

        # Convert the list of dictionaries into a DataFrame
        tweet_columns = {
            "tweet_id": "int64",
            "tweet_created_at": "datetime64[ns]",
            "tweet_conversation_id": "int64",
            "tweet_text": "string",
            "tweet_public_metrics": "string",
            "author_id": "int64",
            "author_username": "string",
            "author_location": "string",
            "author_description": "string",
            "author_created_at": "datetime64[ns]",
            "author_public_metrics": "string",
        }
        df = pd.DataFrame(rows)
        df = df.reindex(columns=list(tweet_columns.keys()))

        # Convert 'tweet_created_at' and 'author_created_at' to timezone-naive datetimes
        df["tweet_created_at"] = pd.to_datetime(df["tweet_created_at"], errors="coerce")
        df["tweet_created_at"] = df["tweet_created_at"].dt.tz_localize(None)
        df["author_created_at"] = pd.to_datetime(
            df["author_created_at"], errors="coerce"
        )
        df["author_created_at"] = df["author_created_at"].dt.tz_localize(None)

        # Convert the data types of the columns
        df = df.astype(tweet_columns)

        return df
