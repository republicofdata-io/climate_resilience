import os

import feedparser
import pandas as pd
from dagster import AssetsDefinition, Output, asset

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
def build_media_feed_assets(name: str, slug: str, rss_feed: str) -> AssetsDefinition:
    @asset(
        name=f"{slug}_media_feed",
        description=f"Media feed for {name}",
        compute_kind="python",
    )
    def _asset():
        # Parse the RSS feed
        feed = feedparser.parse(rss_feed)

        # Extract articles and convert them into a DataFrame
        articles = []
        for entry in feed.entries:
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

        # Enforce data types
        articles_df = pd.DataFrame(articles)
        articles_df["published_ts"] = pd.to_datetime(
            articles_df["published_ts"], format="%a, %d %b %Y %H:%M:%S %z"
        ).dt.tz_localize(None)
        articles_df = pd.DataFrame(articles_df).astype(media_article_columns)

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
    )
    media_feed_assets.append(media_feed_asset)
