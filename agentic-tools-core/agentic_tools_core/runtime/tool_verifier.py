from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from agentic_tools_core.models import VerificationIssue, VerificationResult

RuleFn = Callable[[dict[str, Any], dict[str, Any], bool], tuple[list[VerificationIssue], dict[str, Any]]]


class ToolOutputVerifier:
    def __init__(self) -> None:
        self._rules: dict[str, RuleFn] = {
            "ashby.get_recent_hires": self._verify_ashby_recent_hires,
            "ashby.get_recent_technical_hires": self._verify_ashby_recent_technical_hires,
            "ashby.search_hires": self._verify_ashby_search_hires,
            "ashby.audit_hire_coverage": self._verify_ashby_audit_hire_coverage,
            "harmonic.find_similar_profiles": self._verify_harmonic_similar_profiles,
            "harmonic.enrich_person": self._verify_harmonic_enrich_person,
            "harmonic.enrich_company": self._verify_harmonic_enrich_company,
            "harmonic.get_people_saved_search_results_with_metadata": self._verify_harmonic_saved_search,
            "harmonic.search_companies_by_natural_language": self._verify_harmonic_company_search,
            "harmonic.get_employees_by_company": self._verify_harmonic_company_employees,
            "harmonic.get_team_network_connections_to_company": self._verify_harmonic_team_connections,
            "metaview.enrich_candidate_profiles": self._verify_metaview_enrichment,
            "gem.create_project": self._verify_gem_create_project,
            "gem.add_profiles_to_project": self._verify_gem_add_profiles,
            "gem.add_candidate_note": self._verify_gem_add_note,
            "gem.set_custom_value": self._verify_gem_set_custom_value,
        }

    def has_rule(self, tool_id: str) -> bool:
        return tool_id in self._rules

    def verify(
        self,
        *,
        tool_id: str,
        tool_input: dict[str, Any],
        output: dict[str, Any],
        preview: bool,
        goal_contract: dict[str, Any] | None = None,
    ) -> VerificationResult:
        rule = self._rules.get(tool_id)
        if rule is None:
            return VerificationResult(
                tool_id=tool_id,
                status="warn",
                preview=preview,
                issues=[_warn("missing_verifier_rule", f"No verifier rule registered for tool '{tool_id}'.")],
                stats={},
                retry_hints=[],
                goal_impact={},
            )

        issues, stats = rule(tool_input, output, preview)
        contract_issues = self._contract_issues(
            tool_id=tool_id,
            tool_input=tool_input,
            output=output,
            preview=preview,
            goal_contract=goal_contract or {},
        )
        issues.extend(contract_issues)
        retry_hints = self._retry_hints(
            tool_id=tool_id,
            tool_input=tool_input,
            output=output,
            issues=issues,
            goal_contract=goal_contract or {},
        )
        goal_impact = self._goal_impact(
            tool_id=tool_id,
            output=output,
            issues=issues,
            goal_contract=goal_contract or {},
        )
        if any(item.severity == "error" for item in issues):
            status = "fail"
        elif any(item.severity == "warn" for item in issues):
            status = "warn"
        else:
            status = "pass"
        return VerificationResult(
            tool_id=tool_id,
            status=status,
            preview=preview,
            issues=issues,
            stats=stats,
            retry_hints=retry_hints,
            goal_impact=goal_impact,
        )

    def _contract_issues(
        self,
        *,
        tool_id: str,
        tool_input: dict[str, Any],
        output: dict[str, Any],
        preview: bool,
        goal_contract: dict[str, Any],
    ) -> list[VerificationIssue]:
        del preview
        if not goal_contract:
            return []
        issues: list[VerificationIssue] = []
        requires_global_latest = bool(goal_contract.get("requires_global_latest_proof", False))
        if not requires_global_latest:
            return issues
        if tool_id not in {"ashby.get_recent_hires", "ashby.get_recent_technical_hires", "ashby.search_hires"}:
            return issues

        diagnostics = output.get("diagnostics")
        if not isinstance(diagnostics, dict):
            return issues
        proof_flags = diagnostics.get("proof_flags")
        global_latest_proven = bool(proof_flags.get("global_latest_proven", False)) if isinstance(proof_flags, dict) else False
        if global_latest_proven:
            return issues

        selection_mode = _as_str(tool_input.get("selection_mode")) or _as_str(diagnostics.get("selection_mode"))
        issues.append(
            _error(
                "global_latest_unproven",
                "Goal requires globally latest hires but this result is not proven globally latest.",
                {
                    "selection_mode": selection_mode or "unknown",
                    "stop_reason": _as_str(diagnostics.get("stop_reason")),
                    "scanned_pages": _as_int(diagnostics.get("scanned_pages"), default=0),
                    "max_scan_pages": _as_int(diagnostics.get("max_scan_pages"), default=0),
                },
            )
        )
        return issues

    def _retry_hints(
        self,
        *,
        tool_id: str,
        tool_input: dict[str, Any],
        output: dict[str, Any],
        issues: list[VerificationIssue],
        goal_contract: dict[str, Any],
    ) -> list[dict[str, Any]]:
        del output
        hints: list[dict[str, Any]] = []
        codes = {item.code for item in issues}
        if "global_latest_unproven" in codes and tool_id.startswith("ashby."):
            current_pages = _as_int(tool_input.get("max_scan_pages"), default=0)
            requested_count = _as_int(tool_input.get("count"), default=_as_int(goal_contract.get("requested_hire_count"), default=10))
            next_pages = max(current_pages + 50, 100)
            hints.append(
                {
                    "action": "retry_with_adjusted_input",
                    "tool_id": tool_id,
                    "suggested_input_patch": {
                        "selection_mode": "global_latest_exact",
                        "sort_by": "hired_at",
                        "sort_order": "desc",
                        "max_scan_pages": min(400, next_pages),
                        "count": max(1, requested_count),
                    },
                    "reason": "Need full-scan style proof to satisfy latest-hire goal.",
                }
            )
        if "returned_fewer_than_requested" in codes and tool_id.startswith("ashby."):
            current_pages = _as_int(tool_input.get("max_scan_pages"), default=0)
            if current_pages > 0:
                hints.append(
                    {
                        "action": "retry_with_adjusted_input",
                        "tool_id": tool_id,
                        "suggested_input_patch": {"max_scan_pages": min(400, current_pages + 50)},
                        "reason": "Result count shortfall may be due to scan budget.",
                    }
                )
        return hints

    def _goal_impact(
        self,
        *,
        tool_id: str,
        output: dict[str, Any],
        issues: list[VerificationIssue],
        goal_contract: dict[str, Any],
    ) -> dict[str, Any]:
        impact = {
            "blocks_goal": False,
            "goal_conditions_checked": [],
            "unmet_conditions": [],
        }
        if not goal_contract:
            return impact
        if tool_id.startswith("ashby."):
            impact["goal_conditions_checked"].append("hire_retrieval_quality")
            if any(item.severity == "error" for item in issues):
                impact["blocks_goal"] = True
                impact["unmet_conditions"].extend([item.code for item in issues if item.severity == "error"])
            diagnostics = output.get("diagnostics")
            if isinstance(diagnostics, dict):
                proof_flags = diagnostics.get("proof_flags")
                proven = bool(proof_flags.get("global_latest_proven", False)) if isinstance(proof_flags, dict) else False
                impact["global_latest_proven"] = proven
        return impact

    def _verify_ashby_recent_hires(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        del preview
        hires = _as_list(output.get("hires"))
        diagnostics = output.get("diagnostics")
        confidence = output.get("confidence")
        return _verify_hire_list_common(
            tool_input=tool_input,
            hires=hires,
            require_technical=False,
            diagnostics=diagnostics,
            confidence=confidence,
        )

    def _verify_ashby_recent_technical_hires(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        del preview
        hires = _as_list(output.get("hires"))
        diagnostics = output.get("diagnostics")
        confidence = output.get("confidence")
        return _verify_hire_list_common(
            tool_input=tool_input,
            hires=hires,
            require_technical=True,
            diagnostics=diagnostics,
            confidence=confidence,
        )

    def _verify_ashby_search_hires(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        del preview
        hires = _as_list(output.get("hires"))
        diagnostics = output.get("diagnostics")
        confidence = output.get("confidence")
        filters = tool_input.get("filters")
        technical_only = bool(filters.get("technical_only", False)) if isinstance(filters, dict) else False
        return _verify_hire_list_common(
            tool_input=tool_input,
            hires=hires,
            require_technical=technical_only,
            diagnostics=diagnostics,
            confidence=confidence,
        )

    def _verify_ashby_audit_hire_coverage(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        del preview
        issues: list[VerificationIssue] = []
        diagnostics = output.get("diagnostics")
        confidence = output.get("confidence")
        sample_hires = _as_list(output.get("sample_hires"))
        coverage = output.get("coverage")
        sample_size = _as_int(tool_input.get("sample_size"), default=0)

        issues.extend(_verify_ashby_diagnostics(tool_input=tool_input, diagnostics=diagnostics, confidence=confidence))
        issues.extend(_identity_quality_issues(sample_hires, field_name="sample_hires"))

        if not isinstance(coverage, dict):
            issues.append(_error("missing_coverage", "Ashby coverage audit output missing coverage object."))
            coverage = {}
        returned_count = _as_int(coverage.get("returned_count"), default=len(sample_hires))
        if returned_count < len(sample_hires):
            issues.append(
                _error(
                    "coverage_underflow",
                    "coverage.returned_count is smaller than sample_hires length.",
                    {"returned_count": returned_count, "sample_hires_length": len(sample_hires)},
                )
            )
        if sample_size > 0 and returned_count > sample_size:
            issues.append(
                _error(
                    "coverage_overflow",
                    "coverage.returned_count exceeds requested sample_size.",
                    {"sample_size": sample_size, "returned_count": returned_count},
                )
            )
        return issues, {"sample_size": sample_size, "returned_count": returned_count}

    def _verify_harmonic_similar_profiles(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        del preview
        issues: list[VerificationIssue] = []
        candidates = _as_list(output.get("candidates"))
        seed_profiles = _as_list(tool_input.get("seed_profiles"))
        per_seed = _as_int(tool_input.get("per_seed"), default=0)
        dedupe_report = output.get("dedupe_report")
        if not isinstance(dedupe_report, dict):
            issues.append(_warn("missing_dedupe_report", "Expected dedupe_report object on harmonic similar output."))
            dedupe_report = {}

        if seed_profiles and not candidates:
            issues.append(_warn("empty_result", "No similar profiles returned for non-empty seed profiles."))
        if seed_profiles and per_seed > 0 and len(candidates) > len(seed_profiles) * per_seed:
            issues.append(
                _error(
                    "unexpected_result_size",
                    "Similar profiles output exceeds seed_count * per_seed upper bound.",
                    {
                        "seed_count": len(seed_profiles),
                        "per_seed": per_seed,
                        "output_count": len(candidates),
                    },
                )
            )

        output_count = dedupe_report.get("output_count")
        if isinstance(output_count, int) and output_count != len(candidates):
            issues.append(
                _error(
                    "dedupe_output_mismatch",
                    "dedupe_report.output_count does not match candidates length.",
                    {"dedupe_output_count": output_count, "candidates_length": len(candidates)},
                )
            )

        issues.extend(_identity_quality_issues(candidates, field_name="candidates"))
        return issues, {"seed_count": len(seed_profiles), "output_count": len(candidates)}

    def _verify_harmonic_enrich_person(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        del tool_input, preview
        issues: list[VerificationIssue] = []
        status = _as_str(output.get("status"))
        enrichment_urn = _as_str(output.get("enrichment_urn"))
        person_urn = _as_str(output.get("enriched_person_urn"))
        if not status:
            issues.append(_warn("missing_status", "Harmonic person enrichment response is missing status."))
        if not enrichment_urn and not person_urn:
            issues.append(
                _error(
                    "missing_enrichment_identity",
                    "Harmonic person enrichment response missing both enrichment_urn and enriched_person_urn.",
                )
            )
        return issues, {"status": status}

    def _verify_harmonic_enrich_company(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        del tool_input, preview
        issues: list[VerificationIssue] = []
        status = _as_str(output.get("status"))
        enrichment_urn = _as_str(output.get("enrichment_urn"))
        company_urn = _as_str(output.get("enriched_company_urn"))
        if not status:
            issues.append(_warn("missing_status", "Harmonic company enrichment response is missing status."))
        if not enrichment_urn and not company_urn:
            issues.append(
                _error(
                    "missing_enrichment_identity",
                    "Harmonic company enrichment response missing both enrichment_urn and enriched_company_urn.",
                )
            )
        return issues, {"status": status}

    def _verify_harmonic_saved_search(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        del preview
        issues: list[VerificationIssue] = []
        requested_id = _as_str(tool_input.get("saved_search_id_or_urn"))
        returned_id = _as_str(output.get("saved_search_id_or_urn"))
        candidates = _as_list(output.get("candidates"))
        count = _as_int(output.get("count"), default=len(candidates))
        if requested_id and returned_id and requested_id != returned_id:
            issues.append(
                _error(
                    "saved_search_id_mismatch",
                    "saved_search_id_or_urn in output does not match input.",
                    {"input": requested_id, "output": returned_id},
                )
            )
        if count < len(candidates):
            issues.append(
                _error(
                    "count_underflow",
                    "Output count is smaller than candidates list length.",
                    {"count": count, "candidates_length": len(candidates)},
                )
            )
        issues.extend(_identity_quality_issues(candidates, field_name="candidates"))
        return issues, {"count": count, "candidates_length": len(candidates)}

    def _verify_harmonic_company_search(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        del preview
        issues: list[VerificationIssue] = []
        input_query = _as_str(tool_input.get("query"))
        output_query = _as_str(output.get("query"))
        companies = _as_list(output.get("companies"))
        count = _as_int(output.get("count"), default=len(companies))
        if input_query and output_query and input_query != output_query:
            issues.append(
                _error(
                    "query_mismatch",
                    "Output query does not match input query.",
                    {"input_query": input_query, "output_query": output_query},
                )
            )
        if count < len(companies):
            issues.append(
                _error(
                    "count_underflow",
                    "Output count is smaller than companies list length.",
                    {"count": count, "companies_length": len(companies)},
                )
            )
        missing_identity = [idx for idx, item in enumerate(companies) if not _company_identity(item)]
        if missing_identity:
            issues.append(
                _error(
                    "company_identity_missing",
                    "Some companies are missing identity fields (company_id/company_urn/name).",
                    {"indexes": missing_identity[:20]},
                )
            )
        return issues, {"count": count, "companies_length": len(companies)}

    def _verify_harmonic_company_employees(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        del preview
        issues: list[VerificationIssue] = []
        requested = _as_str(tool_input.get("company_id_or_urn"))
        returned = _as_str(output.get("company_id_or_urn"))
        employees = _as_list(output.get("employees"))
        count = _as_int(output.get("count"), default=len(employees))
        if requested and returned and requested != returned:
            issues.append(
                _error(
                    "company_id_mismatch",
                    "Output company_id_or_urn does not match input.",
                    {"input": requested, "output": returned},
                )
            )
        if count < len(employees):
            issues.append(
                _error(
                    "count_underflow",
                    "Output count is smaller than employees list length.",
                    {"count": count, "employees_length": len(employees)},
                )
            )
        issues.extend(_identity_quality_issues(employees, field_name="employees"))
        return issues, {"count": count, "employees_length": len(employees)}

    def _verify_harmonic_team_connections(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        del preview
        issues: list[VerificationIssue] = []
        requested = _as_str(tool_input.get("company_id_or_urn"))
        returned = _as_str(output.get("company_id_or_urn"))
        connections = _as_list(output.get("connections"))
        count = _as_int(output.get("count"), default=len(connections))
        source_endpoint = _as_str(output.get("source_endpoint"))
        if requested and returned and requested != returned:
            issues.append(
                _error(
                    "company_id_mismatch",
                    "Output company_id_or_urn does not match input.",
                    {"input": requested, "output": returned},
                )
            )
        if count < len(connections):
            issues.append(
                _error(
                    "count_underflow",
                    "Output count is smaller than connections list length.",
                    {"count": count, "connections_length": len(connections)},
                )
            )
        if not source_endpoint:
            issues.append(_warn("missing_source_endpoint", "Connections output missing source_endpoint."))
        for idx, item in enumerate(connections):
            if not isinstance(item, dict):
                issues.append(_error("invalid_connection_item", "Connection entry must be an object.", {"index": idx}))
                continue
            if not _as_str(item.get("connection_id")) and not _as_str(item.get("candidate_id")):
                issues.append(
                    _error(
                        "connection_identity_missing",
                        "Connection entry missing both connection_id and candidate_id.",
                        {"index": idx},
                    )
                )
        return issues, {"count": count, "connections_length": len(connections)}

    def _verify_metaview_enrichment(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        del preview
        issues: list[VerificationIssue] = []
        input_profiles = _as_list(tool_input.get("profiles"))
        candidates = _as_list(output.get("candidates"))
        dedupe_report = output.get("dedupe_report")
        if not isinstance(dedupe_report, dict):
            issues.append(_warn("missing_dedupe_report", "Expected dedupe_report object on Metaview enrichment output."))
            dedupe_report = {}

        if input_profiles and not candidates:
            issues.append(_warn("empty_result", "No enriched profiles returned for non-empty input profiles."))
        if input_profiles and len(candidates) > len(input_profiles):
            issues.append(
                _error(
                    "unexpected_result_size",
                    "Enriched candidates list is larger than input profiles list.",
                    {"input_count": len(input_profiles), "output_count": len(candidates)},
                )
            )

        output_count = dedupe_report.get("output_count")
        if isinstance(output_count, int) and output_count != len(candidates):
            issues.append(
                _error(
                    "dedupe_output_mismatch",
                    "dedupe_report.output_count does not match candidates length.",
                    {"dedupe_output_count": output_count, "candidates_length": len(candidates)},
                )
            )
        issues.extend(_identity_quality_issues(candidates, field_name="candidates"))
        return issues, {"input_count": len(input_profiles), "output_count": len(candidates)}

    def _verify_gem_create_project(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        issues: list[VerificationIssue] = []
        project_id = _as_str(output.get("project_id"))
        name = _as_str(output.get("name"))
        input_name = _as_str(tool_input.get("project_name"))
        if not project_id:
            issues.append(_error("missing_project_id", "Gem project output is missing project_id."))
        if not name:
            issues.append(_error("missing_project_name", "Gem project output is missing name."))
        if input_name and name and input_name != name:
            issues.append(
                _warn(
                    "project_name_mismatch",
                    "Gem project output name does not exactly match input project_name.",
                    {"input": input_name, "output": name},
                )
            )
        if preview and project_id and not project_id.startswith("preview_"):
            issues.append(_warn("unexpected_preview_id", "Preview project_id does not use preview_ prefix."))
        if not preview and project_id.startswith("preview_"):
            issues.append(_error("preview_id_in_live_output", "Live project output should not contain preview project_id."))
        return issues, {"project_id": project_id, "name": name}

    def _verify_gem_add_profiles(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        issues: list[VerificationIssue] = []
        project_id = _as_str(output.get("project_id"))
        input_project_id = _as_str(tool_input.get("project_id"))
        profiles = _as_list(tool_input.get("profiles"))
        added = [item for item in _as_list(output.get("added_candidate_ids")) if _as_str(item)]
        mapping = _as_list(output.get("mapping"))

        if not project_id:
            issues.append(_error("missing_project_id", "Gem add_profiles output is missing project_id."))
        if input_project_id and project_id and not preview and project_id != input_project_id:
            issues.append(
                _warn(
                    "project_id_rewritten",
                    "Gem add_profiles output project_id differs from input (possible expected materialization).",
                    {"input_project_id": input_project_id, "output_project_id": project_id},
                )
            )
        if not preview and project_id.startswith("preview_"):
            issues.append(_error("preview_id_in_live_output", "Live add_profiles output contains preview project_id."))
        if len(set(added)) != len(added):
            issues.append(_error("duplicate_added_candidate_ids", "added_candidate_ids contains duplicates."))
        if profiles and not added:
            issues.append(_warn("empty_addition", "No candidates were added despite non-empty input profiles."))
        if mapping and len(mapping) < len(added):
            issues.append(
                _error(
                    "mapping_underflow",
                    "mapping length is smaller than added_candidate_ids length.",
                    {"mapping_length": len(mapping), "added_length": len(added)},
                )
            )
        for idx, item in enumerate(mapping):
            if not isinstance(item, dict):
                issues.append(_error("invalid_mapping_item", "mapping item must be an object.", {"index": idx}))
                continue
            if not _as_str(item.get("gem_candidate_id")):
                issues.append(_error("missing_gem_candidate_id", "mapping item missing gem_candidate_id.", {"index": idx}))
            if not _as_str(item.get("source_candidate_id")):
                issues.append(
                    _warn(
                        "missing_source_reference",
                        "mapping item missing source_candidate_id; traceability is reduced.",
                        {"index": idx},
                    )
                )
        return issues, {"input_profiles": len(profiles), "added_candidate_ids": len(added), "mapping": len(mapping)}

    def _verify_gem_add_note(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        issues: list[VerificationIssue] = []
        input_candidate_id = _as_str(tool_input.get("candidate_id"))
        output_candidate_id = _as_str(output.get("candidate_id"))
        input_note = _as_str(tool_input.get("note"))
        output_note = _as_str(output.get("note"))
        if not output_candidate_id:
            issues.append(_error("missing_candidate_id", "Gem note output is missing candidate_id."))
        if input_candidate_id and output_candidate_id and input_candidate_id != output_candidate_id:
            issues.append(
                _error(
                    "candidate_id_mismatch",
                    "Gem note output candidate_id does not match input.",
                    {"input": input_candidate_id, "output": output_candidate_id},
                )
            )
        if input_note and output_note and input_note != output_note:
            issues.append(
                _error(
                    "note_mismatch",
                    "Gem note output note does not match input note.",
                    {"input": input_note[:100], "output": output_note[:100]},
                )
            )
        if preview and not bool(output.get("provider_response", {}).get("preview", False)):
            issues.append(_warn("missing_preview_marker", "Preview output missing provider_response.preview=true."))
        return issues, {"candidate_id": output_candidate_id}

    def _verify_gem_set_custom_value(
        self, tool_input: dict[str, Any], output: dict[str, Any], preview: bool
    ) -> tuple[list[VerificationIssue], dict[str, Any]]:
        issues: list[VerificationIssue] = []
        input_candidate_id = _as_str(tool_input.get("candidate_id"))
        output_candidate_id = _as_str(output.get("candidate_id"))
        input_key = _as_str(tool_input.get("key"))
        output_key = _as_str(output.get("key"))
        custom_field_id = _as_str(output.get("custom_field_id"))
        if not output_candidate_id:
            issues.append(_error("missing_candidate_id", "Gem custom value output missing candidate_id."))
        if input_candidate_id and output_candidate_id and input_candidate_id != output_candidate_id:
            issues.append(
                _error(
                    "candidate_id_mismatch",
                    "Gem custom value output candidate_id does not match input.",
                    {"input": input_candidate_id, "output": output_candidate_id},
                )
            )
        if input_key and output_key and input_key != output_key:
            issues.append(
                _error(
                    "key_mismatch",
                    "Gem custom value output key does not match input key.",
                    {"input": input_key, "output": output_key},
                )
            )
        if not custom_field_id:
            issues.append(_error("missing_custom_field_id", "Gem custom value output missing custom_field_id."))
        if preview and not bool(output.get("provider_response", {}).get("preview", False)):
            issues.append(_warn("missing_preview_marker", "Preview output missing provider_response.preview=true."))
        return issues, {"candidate_id": output_candidate_id, "key": output_key}


def _verify_hire_list_common(
    *,
    tool_input: dict[str, Any],
    hires: list[dict[str, Any]],
    require_technical: bool,
    diagnostics: Any,
    confidence: Any,
) -> tuple[list[VerificationIssue], dict[str, Any]]:
    issues: list[VerificationIssue] = []
    requested_count = _as_int(tool_input.get("count"), default=0)
    sort_order = _as_str(tool_input.get("sort_order")).lower() or "desc"
    sort_by = _as_str(tool_input.get("sort_by")).lower() or "hired_at"

    issues.extend(
        _verify_ashby_diagnostics(tool_input=tool_input, diagnostics=diagnostics, confidence=confidence)
    )

    if requested_count > 0 and len(hires) > requested_count:
        issues.append(
            _error(
                "result_count_exceeds_request",
                "Hires list length exceeds requested count.",
                {"requested_count": requested_count, "returned_count": len(hires)},
            )
        )
    if requested_count > 0 and len(hires) < requested_count:
        issues.append(
            _warn(
                "returned_fewer_than_requested",
                "Returned fewer hires than requested.",
                {"requested_count": requested_count, "returned_count": len(hires)},
            )
        )

    issues.extend(_identity_quality_issues(hires, field_name="hires"))

    timestamps = [_extract_hire_timestamp(item, sort_by=sort_by) for item in hires]
    if len(timestamps) >= 2 and all(item is not None for item in timestamps):
        for idx in range(len(timestamps) - 1):
            is_out_of_order = (
                timestamps[idx] < timestamps[idx + 1] if sort_order == "desc" else timestamps[idx] > timestamps[idx + 1]
            )
            if is_out_of_order:
                issues.append(
                    _warn(
                        "not_sorted_by_expected_order",
                        "Hires are not sorted according to requested sort order.",
                        {"index": idx, "sort_by": sort_by, "sort_order": sort_order},
                    )
                )
                break
    elif hires:
        issues.append(
            _warn(
                "missing_hire_timestamps",
                "Unable to verify sorting because comparable hire timestamps are missing.",
                {"returned_count": len(hires), "sort_by": sort_by},
            )
        )

    if require_technical:
        non_technical_indexes: list[int] = []
        for idx, item in enumerate(hires):
            if not _has_technical_signal(item):
                non_technical_indexes.append(idx)
        if non_technical_indexes:
            issues.append(
                _warn(
                    "non_technical_hires_present",
                    "Technical hires output includes entries without clear technical signals.",
                    {"indexes": non_technical_indexes[:20]},
                )
            )

    return issues, {"requested_count": requested_count, "returned_count": len(hires), "sort_by": sort_by, "sort_order": sort_order}


def _verify_ashby_diagnostics(
    *,
    tool_input: dict[str, Any],
    diagnostics: Any,
    confidence: Any,
) -> list[VerificationIssue]:
    issues: list[VerificationIssue] = []
    if not isinstance(diagnostics, dict):
        issues.append(_error("missing_diagnostics", "Ashby output missing diagnostics object."))
        diagnostics = {}

    requested_count = _as_int(tool_input.get("count"), default=_as_int(tool_input.get("sample_size"), default=0))
    diagnostics_requested = _as_int(diagnostics.get("requested_count"), default=requested_count)
    diagnostics_returned = _as_int(diagnostics.get("returned_count"), default=0)
    scanned_pages = _as_int(diagnostics.get("scanned_pages"), default=-1)
    stop_reason = _as_str(diagnostics.get("stop_reason"))

    if requested_count > 0 and diagnostics_requested != requested_count:
        issues.append(
            _warn(
                "diagnostics_request_mismatch",
                "diagnostics.requested_count does not match tool input request.",
                {"input_requested_count": requested_count, "diagnostics_requested_count": diagnostics_requested},
            )
        )
    if diagnostics_returned < 0:
        issues.append(_error("invalid_returned_count", "diagnostics.returned_count must be >= 0."))
    if scanned_pages < 0:
        issues.append(_warn("missing_scanned_pages", "diagnostics.scanned_pages is missing or invalid."))
    if not stop_reason:
        issues.append(_warn("missing_stop_reason", "diagnostics.stop_reason is missing."))
    if not isinstance(diagnostics.get("quality_flags"), list):
        issues.append(_warn("missing_quality_flags", "diagnostics.quality_flags should be a list."))

    parsed_confidence = _as_float(confidence, default=-1.0)
    if parsed_confidence < 0:
        issues.append(_warn("missing_confidence", "Ashby output missing confidence value."))
    elif parsed_confidence > 1:
        issues.append(_error("invalid_confidence", "Ashby output confidence must be <= 1.0."))

    return issues


def _identity_quality_issues(items: list[dict[str, Any]], *, field_name: str) -> list[VerificationIssue]:
    issues: list[VerificationIssue] = []
    seen: set[str] = set()
    duplicates: list[int] = []
    missing_identity: list[int] = []
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            issues.append(_error("invalid_item_type", f"{field_name}[{idx}] is not an object.", {"index": idx}))
            continue
        key = _identity_key(item)
        if not key:
            missing_identity.append(idx)
            continue
        if key in seen:
            duplicates.append(idx)
        seen.add(key)

    if missing_identity:
        issues.append(
            _error(
                "missing_identity",
                f"Some {field_name} entries are missing identity fields.",
                {"indexes": missing_identity[:20]},
            )
        )
    if duplicates:
        issues.append(
            _error(
                "duplicate_entries",
                f"Some {field_name} entries appear duplicated by identity.",
                {"indexes": duplicates[:20]},
            )
        )
    return issues


def _identity_key(item: dict[str, Any]) -> str:
    for key in ("candidate_id", "person_urn", "company_urn", "email", "linkedin", "name"):
        value = _as_str(item.get(key))
        if value:
            return f"{key}:{value.lower()}" if key in {"email", "linkedin", "name"} else f"{key}:{value}"
    return ""


def _company_identity(item: Any) -> str:
    if not isinstance(item, dict):
        return ""
    for key in ("company_id", "company_urn", "name"):
        value = _as_str(item.get(key))
        if value:
            return value
    return ""


def _has_technical_signal(hire: dict[str, Any]) -> bool:
    corpus_parts: list[str] = []
    if isinstance(hire, dict):
        corpus_parts.extend(
            [
                _as_str(hire.get("job_title")),
                " ".join([_as_str(item) for item in _as_list(hire.get("skills"))]),
            ]
        )
        raw = hire.get("raw")
        if isinstance(raw, dict):
            corpus_parts.append(str(raw).lower())
    corpus = " ".join(corpus_parts).lower()
    signals = [
        "engineer",
        "engineering",
        "technical",
        "software",
        "kernel",
        "ml",
        "machine learning",
        "ai",
        "data",
        "platform",
        "backend",
        "frontend",
        "infra",
        "infrastructure",
        "research",
    ]
    return any(token in corpus for token in signals)


def _extract_hire_timestamp(hire: dict[str, Any], *, sort_by: str) -> float | None:
    if not isinstance(hire, dict):
        return None
    raw = hire.get("raw")
    application: dict[str, Any] = {}
    if isinstance(raw, dict):
        app = raw.get("application")
        if isinstance(app, dict):
            application = app

    if sort_by == "created_at":
        candidates = [
            _as_str(hire.get("created_at")),
            _as_str(application.get("createdAt")),
            _as_str(hire.get("hired_at")),
            _as_str(application.get("hiredAt")),
            _as_str(application.get("updatedAt")),
        ]
    elif sort_by == "updated_at":
        candidates = [
            _as_str(hire.get("updated_at")),
            _as_str(application.get("updatedAt")),
            _as_str(hire.get("hired_at")),
            _as_str(application.get("hiredAt")),
            _as_str(application.get("createdAt")),
        ]
    else:
        candidates = [
            _as_str(hire.get("hired_at")),
            _as_str(application.get("hiredAt")),
            _as_str(application.get("hiredDate")),
            _as_str(application.get("updatedAt")),
            _as_str(application.get("createdAt")),
        ]
    for value in candidates:
        if not value:
            continue
        parsed = _parse_iso_ts(value)
        if parsed is not None:
            return parsed
    return None


def _parse_iso_ts(value: str) -> float | None:
    text = value.strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _as_str(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _as_int(value: Any, *, default: int) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except Exception:
            return default
    return default


def _as_float(value: Any, *, default: float) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except Exception:
            return default
    return default


def _error(code: str, message: str, details: dict[str, Any] | None = None) -> VerificationIssue:
    return VerificationIssue(code=code, severity="error", message=message, details=details or {})


def _warn(code: str, message: str, details: dict[str, Any] | None = None) -> VerificationIssue:
    return VerificationIssue(code=code, severity="warn", message=message, details=details or {})
