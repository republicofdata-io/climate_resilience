import pandas as pd
from supabase import create_client


class SupabaseResource:
    def __init__(self, url: str, key: str):
        self.supabase = create_client(url, key)

    def get_media_feeds(self):
        response = (
            self.supabase.table("media_feeds")
            .select("*")
            .eq("is_enabled", "TRUE")
            .execute()
        )
        response_df = pd.DataFrame(response.data)
        return response_df
