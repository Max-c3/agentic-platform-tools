from __future__ import annotations

from agentic_tools_core.runtime.tool_verifier import ToolOutputVerifier


def test_hired_at_verification_does_not_fall_back_to_updated_at() -> None:
    verifier = ToolOutputVerifier()

    result = verifier.verify(
        tool_id="ashby.search_hires",
        tool_input={
            "count": 2,
            "sort_by": "hired_at",
            "sort_order": "desc",
            "filters": {"status": ["hired"]},
        },
        output={
            "hires": [
                {
                    "candidate_id": "cand-1",
                    "name": "Candidate One",
                    "hired_at": "",
                    "raw": {"application": {"updatedAt": "2026-02-01T00:00:00Z"}},
                },
                {
                    "candidate_id": "cand-2",
                    "name": "Candidate Two",
                    "hired_at": "",
                    "raw": {"application": {"updatedAt": "2025-01-01T00:00:00Z"}},
                },
            ],
            "diagnostics": {
                "requested_count": 2,
                "returned_count": 2,
                "scanned_pages": 1,
                "stop_reason": "source_exhausted",
                "quality_flags": ["ok"],
                "proof_flags": {"global_latest_proven": True},
            },
            "confidence": 1.0,
        },
        preview=False,
    )

    assert any(issue.code == "missing_hire_timestamps" for issue in result.issues)
    assert not any(issue.code == "not_sorted_by_expected_order" for issue in result.issues)
