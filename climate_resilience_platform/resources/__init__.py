import os

from .supabase_resource import SupabaseResource
from .x_resource import XResource

supabase_resource = SupabaseResource(
    url=os.environ["SUPABASE_URL"], key=os.environ["SUPABASE_KEY"]
)

x_resource = XResource(x_bearer_token=os.environ.get("X_BEARER_TOKEN", ""))
