from __future__ import annotations

from functools import lru_cache

from agentic_tools_core.common.mode import integration_mode
from agentic_tools_harmonic.client import build_harmonic_client


@lru_cache(maxsize=1)
def get_harmonic_client():
    return build_harmonic_client(integration_mode())
