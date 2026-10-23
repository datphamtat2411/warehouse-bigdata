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
