import os
from datetime import datetime

import feedparser
import pandas as pd
from dagster import AssetExecutionContext, AssetsDefinition, Output, asset

from ..partitions import hourly_partition_def
from ..resources import SupabaseResource

media_article_columns = {
    "media": "string",
    "id": "string",
    "title": "string",
    "link": "string",
    "summary": "string",
    "author": "string",
    "tags": "string",
    "medias": "string",
    "published_ts": "datetime64[ns]",
}

# Get media feeds
supabase_resource = SupabaseResource(
    url=os.environ["SUPABASE_URL"], key=os.environ["SUPABASE_KEY"]
)
media_feeds = supabase_resource.get_media_feeds()


# Factory to create media feed assets
def build_media_feed_assets(
    name: str, slug: str, rss_feed: str, categories: str
) -> AssetsDefinition:
    @asset(
        name=f"{slug}_articles",
        key_prefix=["medias"],
        description=f"Media feed for {name}",
        io_manager_key="bigquery_io_manager",
        partitions_def=hourly_partition_def,
        metadata={"partition_expr": "published_ts"},
        output_required=False,
        compute_kind="python",
    )
    def _asset(context: AssetExecutionContext):
        # Get partition's time
        partition_time_str = context.asset_partition_key_for_output()
        partition_time = datetime.strptime(partition_time_str, "%Y-%m-%d-%H:%M")

        # Parse the RSS feed
        feed = feedparser.parse(rss_feed)

        # Extract articles and convert them into a DataFrame
        articles = []

        for entry in feed.entries:
            if categories != "None":
                # Check if the article has at least one matching category
                entry_tags = getattr(entry, "tags", [])
                if not any(
                    category.strip().lower()
                    in (tag.term.strip().lower() for tag in entry_tags)
                    for category in categories.split(",")
                ):
                    continue  # Skip this article if no matching categories are found

            # If categories is None or the article matches the categories, include it
            articles.append(
                {
                    "media": slug,
                    "id": entry.id,
                    "title": entry.title,
                    "link": entry.link,
                    "summary": entry.summary,
                    "author": entry.author if "author" in entry else None,
                    "tags": [tag.term for tag in getattr(entry, "tags", [])],
                    "medias": (
                        [content["url"] for content in entry.media_content]
                        if "media_content" in entry
                        else []
                    ),
                    "published_ts": entry.published,
                }
            )

        if articles:
            # Enforce data types
            articles_df = pd.DataFrame(articles)
            articles_df["published_ts"] = pd.to_datetime(
                articles_df["published_ts"], format="%a, %d %b %Y %H:%M:%S %z"
            ).dt.tz_localize(None)
            articles_df = articles_df.astype(media_article_columns)

            # Keep articles that are within the partition's time
            articles_df = articles_df[
                (articles_df["published_ts"] >= partition_time)
                & (articles_df["published_ts"] < partition_time + pd.Timedelta(hours=1))
            ]

            # Return asset
            yield Output(
                value=articles_df,
                metadata={
                    "num_rows": articles_df.shape[0],
                },
            )

    return _asset


media_feed_assets = []
for _, media_feed in media_feeds.iterrows():
    media_feed_asset = build_media_feed_assets(
        name=str(media_feed["name"]),
        slug=str(media_feed["slug"]),
        rss_feed=str(media_feed["rss_feed"]),
        categories=str(media_feed["categories"]),
    )
    media_feed_assets.append(media_feed_asset)
