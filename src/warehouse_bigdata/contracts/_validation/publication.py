"""Publication metadata, context, version, and application-copy validation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

from ..manifests import APPROVED, PUBLICATION_CONTEXT_FIELDS
from .common import ValidationIssue, _issue, _parse_date


def publication_issues(
    record: Mapping[str, object],
    *,
    context: Mapping[str, object] | None,
    application_copy: Mapping[str, object] | None,
) -> list[ValidationIssue]:
    contract_name = "publication_manifest"
    issues: list[ValidationIssue] = []
    if "dataset_bindings" in record:
        bindings = record["dataset_bindings"]
        if not isinstance(bindings, Mapping) or not bindings:
            issues.append(
                _issue(
                    "MISSING_PUBLICATION_METADATA",
                    contract_name,
                    field="dataset_bindings",
                    message="explicit dataset bindings required",
                )
            )
        for label, copy in (("context", context), ("application_copy", application_copy)):
            if copy is None or copy.get("dataset_bindings") != bindings:
                issues.append(
                    _issue(
                        "PUBLICATION_MISMATCH",
                        contract_name,
                        field=f"{label}.dataset_bindings",
                        message="historical dataset bindings must be copied exactly",
                    )
                )
    if (
        record.get("application_checksum_definition") == "sha256-canonical-application-copy-v1"
        and application_copy is not None
    ):
        payload = (
            json.dumps(application_copy, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        checksum = "sha256:" + hashlib.sha256(payload).hexdigest()
        if checksum != record.get("application_manifest_checksum"):
            issues.append(
                _issue(
                    "PUBLICATION_MISMATCH",
                    contract_name,
                    field="application_manifest_checksum",
                    message="exact application copy checksum differs",
                )
            )
        release = record.get("release_digest_payload")
        if isinstance(release, Mapping):
            payload = (json.dumps(release, sort_keys=True, separators=(",", ":")) + "\n").encode()
            if "sha256:" + hashlib.sha256(payload).hexdigest() != record.get("artifact_checksum"):
                issues.append(
                    _issue(
                        "PUBLICATION_MISMATCH",
                        contract_name,
                        field="artifact_checksum",
                        message="release content digest differs",
                    )
                )
    required_metadata = (
        "publication_id",
        "selected_dataset_manifests",
        "approved_status",
        "application_manifest_checksum",
        "materialization_reference",
    )
    for field_name in required_metadata:
        if field_name not in record or record[field_name] in (None, ""):
            issues.append(
                _issue(
                    "MISSING_PUBLICATION_METADATA",
                    contract_name,
                    field=field_name,
                    message="approved publication metadata is required",
                )
            )

    if record.get("status") != APPROVED or record.get("approved_status") != APPROVED:
        issues.append(
            _issue(
                "INVALID_STATUS",
                contract_name,
                field="approved_status",
                message="publication status and approved_status must both be APPROVED",
            )
        )

    if context is None:
        issues.append(
            _issue(
                "MISSING_PUBLICATION_METADATA",
                contract_name,
                field="context",
                message="publication validation requires one coherent publication context",
            )
        )
    else:
        issues.extend(_publication_context_issues(context))
        if context.get("contract_version") != record.get("contract_version"):
            issues.append(
                _issue(
                    "VERSION_MISMATCH",
                    contract_name,
                    field="contract_version",
                    message="publication context and manifest versions must agree",
                )
            )
        identity_fields = (
            "publication_id",
            "source_snapshot_id",
            "model_run_id",
            "forecast_run_id",
            "scenario_id",
            "decision_run_id",
            "evaluation_run_id",
        )
        for field_name in identity_fields:
            expected = context.get(field_name)
            if expected is None:
                continue
            if record.get(field_name) != expected:
                issues.append(
                    _issue(
                        "LINEAGE_MISMATCH",
                        contract_name,
                        field=field_name,
                        message=f"publication value must match context value {expected!r}",
                    )
                )

        manifest_checksum = context.get("manifest_checksum")
        if manifest_checksum not in (
            record.get("artifact_checksum"),
            record.get("application_manifest_checksum"),
        ):
            issues.append(
                _issue(
                    "PUBLICATION_MISMATCH",
                    contract_name,
                    field="manifest_checksum",
                    message="context checksum must match publication manifest checksums",
                )
            )

    if application_copy is None:
        issues.append(
            _issue(
                "MISSING_PUBLICATION_METADATA",
                contract_name,
                field="application_copy",
                message="application publication metadata copy is required",
            )
        )
    else:
        for field_name in ("publication_id", "manifest_checksum"):
            if field_name not in application_copy:
                issues.append(
                    _issue(
                        "MISSING_PUBLICATION_METADATA",
                        contract_name,
                        field=f"application_copy.{field_name}",
                        message="application copy must preserve publication identity",
                    )
                )
        if application_copy.get("publication_id") != record.get("publication_id"):
            issues.append(
                _issue(
                    "PUBLICATION_MISMATCH",
                    contract_name,
                    field="application_copy.publication_id",
                    message="application copy publication_id must match the manifest",
                )
            )
        if application_copy.get("manifest_checksum") != record.get("artifact_checksum"):
            issues.append(
                _issue(
                    "PUBLICATION_MISMATCH",
                    contract_name,
                    field="application_copy.manifest_checksum",
                    message="application copy checksum must match the manifest artifact checksum",
                )
            )

    for field_name in ("selected_dataset_manifests", "serving_schema_versions"):
        value = record.get(field_name)
        if not isinstance(value, str) or not value:
            continue
        try:
            versions = json.loads(value)
        except json.JSONDecodeError:
            issues.append(
                _issue(
                    "TYPE_MISMATCH",
                    contract_name,
                    field=field_name,
                    message="publication version metadata must be a JSON object string",
                )
            )
            continue
        if not isinstance(versions, dict) or not versions:
            issues.append(
                _issue(
                    "MISSING_PUBLICATION_METADATA",
                    contract_name,
                    field=field_name,
                    message="publication version metadata must contain at least one dataset",
                )
            )
            continue
        for dataset_name, version in versions.items():
            if not isinstance(dataset_name, str) or version != record.get("contract_version"):
                issues.append(
                    _issue(
                        "VERSION_MISMATCH",
                        contract_name,
                        field=field_name,
                        message="selected dataset versions must match the publication contract version",
                    )
                )
    return issues


def _publication_context_issues(context: Mapping[str, object]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    required_fields = set(PUBLICATION_CONTEXT_FIELDS) - {"evaluation_run_id", "holdout_id"}
    for field_name in sorted(required_fields):
        if field_name not in context or context[field_name] in (None, ""):
            issues.append(
                _issue(
                    "MISSING_PUBLICATION_METADATA",
                    "publication_manifest",
                    field=f"context.{field_name}",
                    message="publication context field is required",
                )
            )
    if "issue_date" in context and _parse_date(context["issue_date"]) is None:
        issues.append(
            _issue(
                "TYPE_MISMATCH",
                "publication_manifest",
                field="context.issue_date",
                message="publication context issue_date must be an ISO date",
            )
        )
    policy_ids = context.get("policy_ids")
    if (
        not isinstance(policy_ids, list)
        or not policy_ids
        or not all(isinstance(policy_id, str) and policy_id for policy_id in policy_ids)
    ):
        issues.append(
            _issue(
                "TYPE_MISMATCH",
                "publication_manifest",
                field="context.policy_ids",
                message="policy_ids must be a non-empty list of identifiers",
            )
        )
    coverage = context.get("coverage")
    if not isinstance(coverage, Mapping):
        issues.append(
            _issue(
                "TYPE_MISMATCH",
                "publication_manifest",
                field="context.coverage",
                message="coverage must be a mapping of measured publication counts",
            )
        )
    return issues
