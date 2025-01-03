import os
from datetime import datetime
from typing import List, TypedDict

import pandas as pd
import requests
import spacy
from dagster import AssetIn, Output, TimeWindowPartitionMapping, asset
from dagster_gcp import BigQueryResource

from ...partitions import three_hour_partition_def
from ...resources.proxycurl_resource import ProxycurlResource

spacy.cli.download("en_core_web_sm")
nlp = spacy.load("en_core_web_sm")


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
    if response.ok and response.json()["geonames"]:
        return response.json()["geonames"][0]
    return {}


def extract_locations_with_spacy(text: str, nlp) -> List[str]:
    """Extract geographical entities from text using spaCy."""
    doc = nlp(text)
    return [ent.text for ent in doc.ents if ent.label_ == "GPE"]


def add_placeholder_geolocation(row, partition_time):
    """Create a placeholder geolocation entry for a user."""
    return SocialNetworkUserProfileGeolocation(
        social_network_profile_id=row["author_id"],
        social_network_profile_username=row["author_username"],
        location_order=None,
        location=None,
        countryName="",
        countryCode="",
        adminName1="",
        adminCode1="",
        latitude=None,
        longitude=None,
        geolocation_ts=datetime.now(),
        partition_hour_utc_ts=partition_time,
    )


def process_locations(
    locations: List[str], row: pd.Series, partition_time: datetime
) -> List[SocialNetworkUserProfileGeolocation]:
    """Process locations and generate geolocation entries."""
    geolocations = []
    for location_order, location in enumerate(locations):
        geocoded_location_data = geocode(location)
        if not geocoded_location_data:
            continue  # Skip if geocode fails
        geolocations.append(
            SocialNetworkUserProfileGeolocation(
                social_network_profile_id=row["author_id"],
                social_network_profile_username=row["author_username"],
                location_order=location_order,
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

    if len(geolocations) == 0:
        geolocations.append(add_placeholder_geolocation(row, partition_time))

    return geolocations


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

    # Combine and deduplicate posts
    social_network_posts = pd.concat(
        [x_conversations, x_conversation_posts], ignore_index=True
    ).drop_duplicates(subset=["author_id"], keep="first")
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

        for _, row in social_network_posts.iterrows():
            try:
                # Extract geographical entities from the user's profile
                locations = extract_locations_with_spacy(
                    row.get("author_location", ""), nlp
                )

                # If locations found via NLP, process them
                if locations:
                    social_network_user_geolocations.extend(
                        process_locations(locations, row, partition_time)
                    )
                    continue

                # TODO: If no locations or location precision is not at the city level, fall back to ProxyCurl

                # Otherwise, fallback to ProxyCurl
                proxycurl_response_df = proxycurl_resource.get_person_profile(
                    f"https://x.com/{row['author_username']}/"
                )

                if proxycurl_response_df.empty:
                    social_network_user_geolocations.append(
                        add_placeholder_geolocation(row, partition_time)
                    )
                    continue

                # Process ProxyCurl data
                proxycurl_locations = [
                    f"{proxycurl_response_df.get('city', '')}, {proxycurl_response_df.get('state', '')}"
                ]
                if proxycurl_locations:
                    social_network_user_geolocations.extend(
                        process_locations(proxycurl_locations, row, partition_time)
                    )
                    continue
                else:
                    social_network_user_geolocations.append(
                        add_placeholder_geolocation(row, partition_time)
                    )

            except Exception as e:
                context.log.error(f"Error processing {row['author_id']}: {e}")
                social_network_user_geolocations.append(
                    add_placeholder_geolocation(row, partition_time)
                )

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
