from __future__ import annotations

from typing import Any


def deduplicate_profiles(profiles: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Deduplicate candidate profiles with deterministic identity precedence:
    email -> linkedin -> candidate_id.
    """
    index: dict[str, dict[str, Any]] = {}
    merged_count = 0

    for profile in profiles:
        email = str(profile.get("email") or "").strip().lower()
        linkedin = str(profile.get("linkedin") or "").strip().lower()
        candidate_id = str(profile.get("candidate_id") or "").strip()

        key = ""
        if email:
            key = f"email:{email}"
        elif linkedin:
            key = f"linkedin:{linkedin}"
        elif candidate_id:
            key = f"id:{candidate_id}"
        else:
            key = f"anonymous:{len(index)+1}"

        if key not in index:
            enriched = dict(profile)
            enriched.setdefault("source_provenance", [])
            if "raw" in profile and isinstance(profile["raw"], dict):
                enriched["source_provenance"].append(profile["raw"])
            index[key] = enriched
            continue

        merged = index[key]
        merged_count += 1
        for field in ["name", "email", "linkedin", "candidate_id"]:
            if not merged.get(field) and profile.get(field):
                merged[field] = profile[field]

        merged_skills = set(merged.get("skills") or [])
        incoming_skills = set(profile.get("skills") or [])
        merged["skills"] = sorted(list(merged_skills | incoming_skills))

        signal = profile.get("metaview_signal")
        if signal and not merged.get("metaview_signal"):
            merged["metaview_signal"] = signal

        if "raw" in profile and isinstance(profile["raw"], dict):
            merged.setdefault("source_provenance", []).append(profile["raw"])

    deduped = list(index.values())
    report = {
        "input_count": len(profiles),
        "output_count": len(deduped),
        "merged_count": merged_count,
    }
    return deduped, report
