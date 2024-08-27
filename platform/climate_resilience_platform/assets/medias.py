import os

import feedparser
import pandas as pd
from dagster import AssetExecutionContext, AssetsDefinition, Output, asset

from ..resources import SupabaseResource

media_article_columns = {
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
        name=f"{slug}_media_feed",
        description=f"Media feed for {name}",
        io_manager_key="bigquery_io_manager",
        output_required=False,
        compute_kind="python",
    )
    def _asset(context: AssetExecutionContext):
        # Parse the RSS feed
        feed = feedparser.parse(rss_feed)

        # Extract articles and convert them into a DataFrame
        articles = []

        for entry in feed.entries:
            if categories != "None":
                # Check if the article has at least one matching category
                if not any(
                    category.strip().lower()
                    in (tag.term.strip().lower() for tag in entry.tags)
                    for category in categories.split(",")
                ):
                    continue  # Skip this article if no matching categories are found

            # If categories is None or the article matches the categories, include it
            articles.append(
                {
                    "id": entry.id,
                    "title": entry.title,
                    "link": entry.link,
                    "summary": entry.summary,
                    "author": entry.author if "author" in entry else None,
                    "tags": [tag.term for tag in entry.tags],
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
