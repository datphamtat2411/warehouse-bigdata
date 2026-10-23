"""Fresh, bounded artifact verification and failure-closed owner publication."""

import copy
import json
from collections import Counter
from pathlib import PurePosixPath
from types import SimpleNamespace

import pytest

from tests.unit.test_feature_snapshot import RUN, _feature_staged
from tests.unit.test_hdfs_namespace import CONFIG, PermissionHdfs
from warehouse_bigdata.common.hdfs_client import HdfsCliClient, HdfsCommandError
from warehouse_bigdata.common.hdfs_namespace import HdfsNamespaceError
from warehouse_bigdata.common.hdfs_publication import (
    file_integrity,
    finalize_publication,
    validate_feature_publication_evidence,
)


@pytest.mark.parametrize('policy', [False, True])
def test_evaluation_dispatch_preserves_forecast_owner(policy, monkeypatch):
    from warehouse_bigdata.common import hdfs_publication as publication
    from warehouse_bigdata.forecasting import evaluation_artifacts as forecast
    from warehouse_bigdata.optimization import inventory_artifacts as inventory
    client = PermissionHdfs()
    from warehouse_bigdata.common.hdfs_namespace import provision_publication_namespace
    provision_publication_namespace(client, CONFIG)
    manifest = dict(contract_name='evaluation_manifest', producer=(inventory.EVALUATION_PRODUCER
        if policy else 'warehouse-bigdata:evaluate-forecasts'))
    monkeypatch.setattr(publication, '_json', lambda *a: dict(version=1, pipeline_run_id='run', manifest=manifest))
    monkeypatch.setattr(inventory, 'finalize_inventory_publication', lambda *a, **k: 'POLICY')
    monkeypatch.setattr(forecast, 'finalize_evaluation_publication', lambda *a, **k: 'FORECAST')
    assert finalize_publication(client, CONFIG, pipeline_run_id='run') == ('POLICY' if policy else 'FORECAST')


def test_34_file_inventory_preserves_every_actual_checksum_with_one_cli(monkeypatch):
    root = "/artifact"
    files = (
        {root + f"/part-{index:05}.parquet": index + 1 for index in range(16)}
        | {
            root + f"/_diagnostics/exclusions/part-{index:05}.parquet": index + 17
            for index in range(16)
        }
        | {root + "/_SUCCESS": 0, root + "/_diagnostics/exclusions/_SUCCESS": 0}
    )
    directories = {root, root + "/_diagnostics", root + "/_diagnostics/exclusions"}
    calls = Counter()
    client = HdfsCliClient()

    def web(path, operation):
        calls[operation] += 1
        entries = []
        for child in sorted(set(files) | directories | {root + "/_dataset_manifest.json"}):
            if str(PurePosixPath(child).parent) == path:
                entries.append(
                    {
                        "pathSuffix": PurePosixPath(child).name,
                        "type": "DIRECTORY" if child in directories else "FILE",
                        "length": files.get(child, 0),
                    }
                )
        return {"FileStatuses": {"FileStatus": entries}}

    def run(args):
        assert args[:2] == ("dfs", "-checksum")
        calls["checksum_processes"] += 1
        return SimpleNamespace(
            stdout="".join(
                f"{path}\tMD5-of-0MD5-of-512CRC32C\t{files[path]:056x}\n" for path in args[2:]
            ).encode()
        )

    monkeypatch.setattr(client, "_webhdfs", web)
    monkeypatch.setattr(client, "_run", run)
    expected = sorted(
        (
            {
                "path": path.removeprefix(root + "/"),
                "size_bytes": size,
                "checksum": f"MD5-of-0MD5-of-512CRC32C\t{size:056x}",
            }
            for path, size in files.items()
        ),
        key=lambda item: item["path"],
    )
    assert file_integrity(client, root) == expected
    assert calls == {"LISTSTATUS": 3, "checksum_processes": 1}
    files[root + "/part-00000.parquet"] = 99
    assert file_integrity(client, root) != expected
    assert calls == {"LISTSTATUS": 6, "checksum_processes": 2}


def feature_client():
    client = PermissionHdfs()
    candidate, final, manifest = _feature_staged(client)
    client.list_status = lambda path: tuple(
        {
            "path": child,
            "type": metadata["type"],
            "length": len(client.files.get(child, b"")),
        }
        for child, metadata in client.entries.items()
        if str(PurePosixPath(child).parent) == path
    )
    client.checksums = lambda paths: {path: client.checksum(path) for path in paths}
    return client, candidate, final, manifest


def test_feature_finalization_uses_fresh_namenode_probes_and_fails_closed_on_denial():
    client, candidate, final, _ = feature_client()
    original_status = client.path_status

    def denied(path):
        if path == candidate:
            raise HdfsCommandError("ancestor access denied")
        return original_status(path)

    client.path_status = denied
    before = copy.deepcopy((client.entries, client.files))
    with pytest.raises(HdfsCommandError, match="denied"):
        finalize_publication(client, CONFIG, pipeline_run_id=RUN)
    assert (client.entries, client.files) == before
    client.path_status = original_status

    def no_cli_probe(path):
        raise AssertionError("J04 should use exact-path NameNode metadata")

    client.exists = no_cli_probe
    assert finalize_publication(client, CONFIG, pipeline_run_id=RUN) == "VALIDATED"
    assert final in client.entries


@pytest.mark.parametrize(
    "corruption",
    [
        "missing",
        "extra",
        "hidden",
        "duplicate",
        "non-child",
        "symlink",
        "size",
        "checksum",
        "denied-list",
        "denied-checksum",
        "checksum-missing",
        "checksum-extra",
    ],
)
def test_feature_evidence_rejects_invalid_actual_inventory(corruption):
    client, candidate, _, manifest = feature_client()
    original_list = client.list_status
    if corruption == "missing":
        del client.entries[candidate + "/part-00000.parquet"]
    elif corruption in {"extra", "hidden"}:
        name = "extra.parquet" if corruption == "extra" else ".hidden"
        client.upload_bytes(b"unexpected", candidate + "/" + name)
    elif corruption == "checksum":
        client.checksum = lambda path: "actual-changed-checksum"
    elif corruption.startswith("checksum-"):
        client.checksums = lambda paths: (
            {}
            if corruption.endswith("missing")
            else {**{path: "fixture-hash" for path in paths}, "/extra": "fixture-hash"}
        )
    elif corruption.startswith("denied"):

        def denied(*args):
            raise HdfsCommandError("access denied")

        if corruption == "denied-list":
            client.list_status = denied
        else:
            client.checksums = denied
    else:

        def listing(path):
            entries = list(original_list(path))
            if corruption == "duplicate":
                entries.append(entries[0])
            elif corruption == "non-child":
                entries[0] = {**entries[0], "path": path + "/../escape.parquet"}
            else:
                for entry in entries:
                    if entry["path"].endswith(".parquet"):
                        entry["type" if corruption == "symlink" else "length"] = (
                            "SYMLINK" if corruption == "symlink" else 999
                        )
            return entries

        client.list_status = listing
    with pytest.raises((HdfsNamespaceError, HdfsCommandError)):
        validate_feature_publication_evidence(client, CONFIG, manifest, candidate)


def test_interrupt_after_claim_retains_frozen_evidence_and_rechecks_actual_bytes():
    client, candidate, final, _ = feature_client()
    original_checksums = client.checksums
    phases = []

    def checksums(paths):
        phases.append(tuple(paths))
        if len(phases) == 2:
            raise KeyboardInterrupt("owner interrupted after sealing")
        return original_checksums(paths)

    client.checksums = checksums
    request = next(path for path in client.files if path.endswith("/publication/request.json"))
    frozen = client.files[request]
    with pytest.raises(KeyboardInterrupt):
        finalize_publication(client, CONFIG, pipeline_run_id=RUN)
    assert not client.exists(candidate) and not client.exists(final)
    assert client.files[request] == frozen
    assert client.path_status(str(PurePosixPath(phases[1][0]).parent))["permission"] == "555"
    assert finalize_publication(client, CONFIG, pipeline_run_id=RUN) == "VALIDATED"
    assert len(phases) == 4  # Fresh preflight and postseal checks again on recovery.
    before = copy.deepcopy((client.entries, client.files))
    assert finalize_publication(client, CONFIG, pipeline_run_id=RUN) == "REUSED"
    assert len(phases) == 5
    assert (client.entries, client.files) == before
    assert json.loads(client.files[request])


def test_reused_producer_evidence_shares_one_fresh_inventory_per_verification():
    client, _, final, _ = feature_client()
    assert finalize_publication(client, CONFIG, pipeline_run_id=RUN) == "VALIDATED"
    before = {path: data for path, data in client.files.items() if path.startswith(final + "/")}
    next_run = RUN + "-reuse"
    candidate, _, manifest = _feature_staged(client, run_id=next_run)
    client.remove(candidate)
    coverage = json.loads(manifest["coverage_and_exclusion_counts"])
    original_request = next(
        path for path in client.files if path.endswith(RUN + "/publication/request.json")
    )
    original = json.loads(client.files[original_request])["manifest"]
    original_coverage = json.loads(original["coverage_and_exclusion_counts"])
    coverage["producing_pipeline_run_id"] = RUN
    coverage["dataset_manifest_digest"] = original_coverage["dataset_manifest_digest"]
    manifest["coverage_and_exclusion_counts"] = json.dumps(coverage)
    request = original_request.replace(RUN + "/", next_run + "/")
    client.files[request] = json.dumps({"version": 1, "manifest": manifest}).encode()
    calls = []
    checksums = client.checksums

    def counted(paths):
        calls.append(tuple(paths))
        return checksums(paths)

    client.checksums = counted
    assert finalize_publication(client, CONFIG, pipeline_run_id=next_run) == "VALIDATED"
    assert len(calls) == 1
    assert {
        path: data for path, data in client.files.items() if path.startswith(final + "/")
    } == before


def processing_candidate(stage, *, client=None):
    from tests.unit.test_hdfs_raw_ingestion import _run_manifest
    from tests.unit.test_m5_small import evidence
    from warehouse_bigdata.common.hdfs_namespace import provision_publication_namespace
    from warehouse_bigdata.common.hdfs_paths import (
        render_curated_category_monthly_sales_path,
        render_curated_sales_daily_path,
        render_curated_sales_enriched_path,
        render_curated_store_daily_sales_path,
        render_enrichment_candidate_path,
        render_normalization_candidate_path,
        render_run_manifest_path,
        render_staging_path,
    )
    from warehouse_bigdata.common.hdfs_publication import stage_publication
    from warehouse_bigdata.common.scope_evidence import bind_scope, write_scope

    client = client or PermissionHdfs()
    provision_publication_namespace(client, CONFIG)
    scope = evidence()
    run = "small-" + stage
    staging = render_staging_path(CONFIG.base_path, run)
    client.mkdir_exclusive(staging)
    if stage == "j03":
        client.mkdir_exclusive(staging + "/candidates")
    outputs = (
        {"sales_daily": render_curated_sales_daily_path}
        if stage == "j02"
        else {
            "sales_enriched": render_curated_sales_enriched_path,
            "store_daily_sales": render_curated_store_daily_sales_path,
            "category_monthly_sales": render_curated_category_monthly_sales_path,
        }
    )
    paths = {
        name: render(CONFIG.base_path, scope["source_snapshot_id"])
        for name, render in outputs.items()
    }
    manifest_path = render_run_manifest_path(CONFIG.base_path, run)
    manifest = _run_manifest(run, scope["source_version"], CONFIG.uri + manifest_path)
    manifest.update(
        source_snapshot_id=scope["source_snapshot_id"],
        producer="warehouse-bigdata:"
        + ("normalization-snapshot" if stage == "j02" else "enrichment-analytics-snapshot-v1"),
        output_dataset_names=json.dumps(list(outputs)),
        output_dataset_uris=json.dumps([CONFIG.uri + p for p in paths.values()]),
        quality_report_uri=CONFIG.uri + staging + "/quality_report.json",
    )
    for name, final in paths.items():
        candidate = (
            render_normalization_candidate_path(staging)
            if stage == "j02"
            else render_enrichment_candidate_path(staging, name)
        )
        client.mkdir_exclusive(candidate)
        write_scope(client, candidate, scope)
        sidecar = "_snapshot_manifest.json" if stage == "j02" else "_dataset_manifest.json"
        document = {
            key: manifest[key]
            for key in ("status", "pipeline_run_id", "source_version", "source_snapshot_id")
        }
        document["artifact_uri"] = CONFIG.uri + final
        client.upload_bytes(json.dumps(document).encode(), candidate + "/" + sidecar)
    client.upload_bytes(b'{"status":"VALIDATED"}', staging + "/quality_report.json")
    stage_publication(client, CONFIG, bind_scope(manifest, scope), result_status="VALIDATED")
    return client, paths


@pytest.mark.parametrize(
    "stage,job", [("j02", "normalize_sales_daily"), ("j03", "enrich_analytics")]
)
@pytest.mark.parametrize("completed", [False, True])
def test_processing_owner_recovery_verifies_publication_and_skips_driver(
    monkeypatch, stage, job, completed
):
    import sys

    from warehouse_bigdata.common import hdfs_publication as module

    client, paths = processing_candidate(stage)
    events = CONFIG.base_path + "/staging/pipeline_run_id=small-" + stage + "/events"
    client.mkdir_exclusive(events)
    client.upload_bytes(b"original completed application log", events + "/app-original")
    if completed:
        finalize_publication(client, CONFIG, pipeline_run_id="small-" + stage)
    monkeypatch.setattr(module, "HdfsCliClient", lambda **kwargs: client)
    monkeypatch.setattr(module, "load_hdfs_config", lambda: CONFIG)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "publisher",
            "--recover",
            "--skip-completed-driver",
            "--pipeline-run-id",
            "small-" + stage,
            "--job-name",
            job,
            "--source-version",
            "sha256:" + "a" * 64,
            "--source-snapshot-id",
            "small-snapshot",
        ],
    )
    with pytest.raises(SystemExit) as result:
        module.main()
    assert result.value.code == 10
    assert all(client.exists(path) for path in paths.values())
    assert {
        path: payload for path, payload in client.files.items() if path.startswith(events + "/")
    } == {events + "/app-original": b"original completed application log"}


@pytest.mark.parametrize(
    "stage,job", [("j02", "normalize_sales_daily"), ("j03", "enrich_analytics")]
)
@pytest.mark.parametrize(
    "conflict", ["source-version", "source-snapshot-id", "job-name", "missing-source"]
)
def test_processing_recovery_refuses_conflicting_binding_before_publication(
    monkeypatch, stage, job, conflict
):
    import sys

    from warehouse_bigdata.common import hdfs_publication as module

    client, _ = processing_candidate(stage)
    arguments = {
        "pipeline-run-id": "small-" + stage,
        "job-name": job,
        "source-version": "sha256:" + "a" * 64,
        "source-snapshot-id": "small-snapshot",
    }
    if conflict == "missing-source":
        arguments.pop("source-version")
    elif conflict == "job-name":
        arguments[conflict] = (
            "enrich_analytics" if job == "normalize_sales_daily" else "normalize_sales_daily"
        )
    else:
        arguments[conflict] = "conflict"
    monkeypatch.setattr(module, "HdfsCliClient", lambda **kwargs: client)
    monkeypatch.setattr(module, "load_hdfs_config", lambda: CONFIG)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "publisher",
            "--recover",
            "--skip-completed-driver",
            *[arg for key, value in arguments.items() for arg in ("--" + key, value)],
        ],
    )
    before = copy.deepcopy((client.entries, client.files))
    with pytest.raises(HdfsNamespaceError, match="conflict|binding"):
        module.main()
    assert (client.entries, client.files) == before
