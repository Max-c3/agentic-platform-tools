from __future__ import annotations

from functools import lru_cache

from agentic_tools_core.common.mode import integration_mode
from agentic_tools_gem.client import build_gem_client


@lru_cache(maxsize=1)
def get_gem_client():
    return build_gem_client(integration_mode())
