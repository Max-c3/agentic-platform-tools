from __future__ import annotations

from functools import lru_cache

from agentic_tools_core.common.mode import integration_mode
from agentic_tools_metaview.client import build_metaview_client


@lru_cache(maxsize=1)
def get_metaview_client():
    return build_metaview_client(integration_mode())
