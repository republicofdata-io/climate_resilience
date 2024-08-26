from dagster import Definitions

from .resources import *

defs = Definitions(
    resources={
        "supabase_resource": supabase_resource,
    }
)
