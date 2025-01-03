import os
from datetime import datetime
from typing import TypedDict

import pandas as pd
import requests
import spacy
from dagster import AssetIn, Output, TimeWindowPartitionMapping, asset
from dagster_gcp import BigQueryResource

from ...partitions import three_hour_partition_def
from ...resources.proxycurl_resource import ProxycurlResource

spacy.cli.download("en_core_web_sm")


class SocialNetworkUserProfileGeolocation(TypedDict):
    social_network_profile_id: str
    social_network_profile_username: str
    location_order: int
    location: str
    countryName: str
    countryCode: str
    adminName1: str
    adminCode1: str
    latitude: str
    longitude: str
    geolocation_ts: datetime
    partition_hour_utc_ts: datetime


# Geocode locations using GeoNames
def geocode(location):
    spacy_username = os.getenv("SPACY_USERNAME")
    url = f"http://api.geonames.org/searchJSON?q={location}&maxRows=1&username={spacy_username}"
    response = requests.get(url)
    return response.json()["geonames"][0]


# Calculate the number of decimal places in a coordinate.
def calculate_precision(coord):
    if pd.isna(coord) or coord == "":
        return 0
    # Split the coordinate on the decimal point and return the length of the fractional part
    return len(coord.split(".")[1]) if "." in coord else 0


# Calculate the precision for latitude and longitude
def most_precise_location(group):
    group["latitude_precision"] = group["latitude"].apply(calculate_precision)
    group["longitude_precision"] = group["longitude"].apply(calculate_precision)

    # Sum the precision of latitude and longitude to get a total precision score
    group["total_precision"] = (
        group["latitude_precision"] + group["longitude_precision"]
    )

    # Sort the group by total precision in descending order and return the first row
    most_precise = group.sort_values("total_precision", ascending=False).iloc[0]

    # Drop the temporary precision columns before returning
    return most_precise.drop(
        ["latitude_precision", "longitude_precision", "total_precision"]
    )


@asset(
    name="user_geolocations",
    description="Geolocation of social network user's profile location",
    io_manager_key="social_networks_io_manager",
    ins={
        "x_conversations": AssetIn(
            key=["social_networks", "x_conversations"],
        ),
        "x_conversation_posts": AssetIn(
            key=["social_networks", "x_conversation_posts"],
            partition_mapping=TimeWindowPartitionMapping(start_offset=0, end_offset=1),
        ),
    },
    partitions_def=three_hour_partition_def,
    metadata={"partition_expr": "partition_hour_utc_ts"},
    output_required=False,
    compute_kind="python",
)
def user_geolocations(
    context,
    x_conversations,
    x_conversation_posts,
    gcp_resource: BigQueryResource,
    proxycurl_resource: ProxycurlResource,
):
    # Log upstream asset's partition keys
    context.log.info(
        f"Partition key range for x_conversations: {context.asset_partition_key_range_for_input('x_conversations')}"
    )
    context.log.info(
        f"Partition key range for x_conversation_posts: {context.asset_partition_key_range_for_input('x_conversation_posts')}"
    )

    # Get partition's time
    partition_time_str = context.partition_key
    partition_time = datetime.strptime(partition_time_str, "%Y-%m-%d-%H:%M")

    # Initialize DataFrame that will hold geolocation data of social network user's profile location
    social_network_user_geolocations = []

    # Concatenate the social network posts
    social_network_posts = pd.concat(
        [x_conversations, x_conversation_posts],
        ignore_index=True,
    )

    # Drop duplicates
    social_network_posts = social_network_posts.drop_duplicates(
        subset=["author_id"], keep="first"
    )
    context.log.info(f"Geolocating {len(social_network_posts)} social network users.")

    # If we have social network posts to geolocate
    if not social_network_posts.empty:
        # Get existing geolocations for list of users
        sql = f"""
        select * from {os.getenv("BIGQUERY_PROJECT_ID")}.{os.getenv("BIGQUERY_SOCIAL_NETWORKS_DATASET")}.user_geolocations
        where social_network_profile_id in ({','.join(map(lambda x: f"'{x}'", social_network_posts["author_id"].to_list()))})
        and geolocation_ts >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 30 DAY)
        """

        with gcp_resource.get_client() as client:
            job = client.query(sql)
            job.result()  # Wait for the job to complete

            if job.error_result:
                error_message = job.error_result.get("message", "Unknown error")
                raise GoogleAPIError(f"BigQuery job failed: {error_message}")
            else:
                existing_user_geolocations_df = job.to_dataframe()

        # Filter out users that have already been geolocated in the past 30 days
        social_network_posts = social_network_posts[
            ~social_network_posts["author_id"].isin(
                existing_user_geolocations_df["SOCIAL_NETWORK_PROFILE_ID"]
            )
        ]
        context.log.info(
            f"Performing new geolocations for {len(social_network_posts)} social network users."
        )

        # First approach: Extract geographical entities from the user's profile

        # Load spaCy's NER model
        nlp = spacy.load("en_core_web_sm")

        for _, row in social_network_posts.iterrows():
            try:
                # Extract location entities
                doc = nlp(str(row["author_location"]))
                locations = [ent.text for ent in doc.ents if ent.label_ == "GPE"]

                for location in locations:
                    geocoded_location_data = geocode(location)

                    social_network_user_geolocations.append(
                        SocialNetworkUserProfileGeolocation(
                            social_network_profile_id=row["author_id"],
                            social_network_profile_username=row["author_username"],
                            location_order=locations.index(location),
                            location=location,
                            countryName=geocoded_location_data.get("countryName", ""),
                            countryCode=geocoded_location_data.get("countryCode", ""),
                            adminName1=geocoded_location_data.get("adminName1", ""),
                            adminCode1=geocoded_location_data.get("adminCode1", ""),
                            latitude=geocoded_location_data.get("lat", ""),
                            longitude=geocoded_location_data.get("lng", ""),
                            geolocation_ts=datetime.now(),
                            partition_hour_utc_ts=partition_time,
                        )
                    )

            except Exception as e:
                print(f"Error geolocating {row['article_url']}: {e}")

        # TODO: Second method is with ProxyCurl resource
        # Filter out users that have been geolocated with the NLP approach
        social_network_posts = social_network_posts[
            ~social_network_posts["author_id"].isin(
                [
                    x["social_network_profile_id"]
                    for x in social_network_user_geolocations
                ]
            )
        ]

        for _, row in social_network_posts.iterrows():
            # TODO: Send requests to ProxyCurl resource
            context.log.info(f"Geolocating {row['author_id']} with ProxyCurl resource.")
            proxycurl_response_df = proxycurl_resource.get_person_profile(
                "https://x.com/" + row["author_username"] + "/"
            )
            context.log.info(f"ProxyCurl response: {proxycurl_response_df}")

            # TODO: If 404, create geolocation entry so that user is not geolocated again for 30 days

            # TODO: Parse the location fields
            # TODO: If the location field is not empty, send request to GeoNames API to get the geolocation data
            # TODO: If the location field is empty, create geolocation entry with null values so that we don't geolocate again for 30 days

    # Deduplicate geolocation info and return asset
    if social_network_user_geolocations:
        social_network_user_profile_geolocations_df = pd.DataFrame(
            social_network_user_geolocations
        )

        # Deduplicate the DataFrame by social_network_profile_id and by keeping the hightest level of precision
        social_network_user_profile_geolocations_df = (
            social_network_user_profile_geolocations_df.groupby(
                "social_network_profile_id", as_index=False
            )
            .apply(most_precise_location)
            .reset_index(drop=True)
        )

        # Return asset
        yield Output(
            value=social_network_user_profile_geolocations_df,
            metadata={
                "num_rows": social_network_user_profile_geolocations_df.shape[0],
            },
        )
