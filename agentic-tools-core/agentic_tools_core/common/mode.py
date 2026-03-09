from __future__ import annotations

import os


def integration_mode() -> str:
    return os.getenv("AR_INTEGRATION_MODE", "mock").strip().lower()
