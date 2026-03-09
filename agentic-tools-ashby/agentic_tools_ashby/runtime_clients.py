from __future__ import annotations

from functools import lru_cache

from agentic_tools_ashby.client import build_ashby_client
from agentic_tools_core.common.mode import integration_mode


@lru_cache(maxsize=1)
def get_ashby_client():
    return build_ashby_client(integration_mode())
