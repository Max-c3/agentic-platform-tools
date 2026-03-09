from __future__ import annotations

import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from agentic_tools_harmonic.catalog import generate_catalog


def main() -> int:
    target = Path("tool_catalog.json")
    expected = json.loads(target.read_text())
    with TemporaryDirectory() as tmpdir:
        generated = generate_catalog(Path(tmpdir) / "tool_catalog.json")
        payload = json.loads(generated.read_text())
    if not payload.get("tools"):
        print("tool catalog is empty", file=sys.stderr)
        return 1
    if payload != expected:
        print("tool_catalog.json is out of date", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
