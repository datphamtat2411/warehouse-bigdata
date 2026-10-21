"""Offline T03 checks: no Docker daemon, Hadoop installation, or project data."""

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.integration import hdfs_support as hdfs
from tests.integration.conftest import pytest_collection_modifyitems, pytest_runtest_makereport

ADDRESSES = {"172.20.0.3:9866", "172.20.0.4:9866"}
DIRECTORY = "/t03-integration-0123456789abcdef0123456789abcdef"
REPORT = """Configured Capacity: 1000000 (976.56 KB)
Live datanodes (2):

Name: 172.20.0.3:9866 (hdfs-datanode-1.warehouse-bigdata)
Hostname: hdfs-datanode-1
Decommission Status : Normal
Last contact: Mon Oct 19 10:00:00 UTC 2026
Last Block Report: Mon Oct 19 09:59:59 UTC 2026

Name: 172.20.0.4:9866 (hdfs-datanode-2.warehouse-bigdata)
Hostname: hdfs-datanode-2
Decommission Status : Normal
Last contact: Mon Oct 19 10:00:01 UTC 2026
Last Block Report: Mon Oct 19 10:00:00 UTC 2026

Dead datanodes (0):
"""


def block_line(number: int = 0, locations: tuple[str, ...] | None = None) -> str:
    if locations is None:
        locations = tuple(sorted(ADDRESSES))
    storage = ", ".join(
        f"DatanodeInfoWithStorage[{address},DS-{index},DISK]"
        for index, address in enumerate(locations)
    )
    return (
        f"{number}. BP-1-172.20.0.2-1:blk_{1073741825 + number}_1001 len=32768 "
        f"Live_repl={len(locations)} [{storage}]"
    )


def fsck(*blocks: str, count: int | None = None) -> str:
    count = len(blocks) if count is None else count
    return (
        f"{DIRECTORY}/payload 65536 bytes, replicated: replication=2, {count} block(s): OK\n"
        + "\n".join(blocks)
        + "\nStatus: HEALTHY\nMissing blocks: 0\nCorrupt blocks: 0\n"
        + "Under-replicated blocks: 0\nMissing replicas: 0 (0.0 %)\n"
    )


def test_live_report_uses_live_section_and_exact_hostnames() -> None:
    nodes = hdfs.parse_report(REPORT)
    assert set(nodes) == set(hdfs.DATANODES)
    assert {node.address for node in nodes.values()} == ADDRESSES
    assert nodes["hdfs-datanode-2"].last_block_report == "Mon Oct 19 10:00:00 UTC 2026"
    # A dead node must never fill a missing live registration.
    dead_only = REPORT.replace("Live datanodes (2):", "Live datanodes (0):\nDead datanodes (2):")
    with pytest.raises(hdfs.VerificationError):
        hdfs.parse_report(dead_only)


@pytest.mark.parametrize(
    "report",
    [
        "",
        REPORT.replace("Live datanodes (2)", "Live datanodes (1)"),
        REPORT.replace("Hostname: hdfs-datanode-2", "Hostname: hdfs-datanode-1"),
        REPORT.replace("Hostname: hdfs-datanode-2", "Hostname: hdfs-datanode-20"),
        REPORT.replace("172.20.0.4:9866", "172.20.0.3:9866"),
        REPORT.replace("Decommission Status : Normal", "Decommission Status : Decommissioned", 1),
    ],
)
def test_invalid_or_non_normal_registration_is_rejected(report: str) -> None:
    with pytest.raises(hdfs.VerificationError):
        hdfs.parse_report(report)


def test_placement_audits_every_block_and_retains_block_identity() -> None:
    blocks = hdfs.parse_fsck(fsck(block_line(), block_line(1)), ADDRESSES)
    assert len(blocks) == 2
    assert blocks[0].block_id == "BP-1-172.20.0.2-1:blk_1073741825_1001"
    assert all(set(block.locations) == ADDRESSES for block in blocks)
    assert sum(block.length for block in blocks) == 65536
    # Placement resolves the current report, including newly allocated container IPs.
    moved = fsck(block_line()).replace("172.20.0.3:9866", "172.20.0.9:9866")
    with pytest.raises(hdfs.VerificationError):
        hdfs.parse_fsck(moved, ADDRESSES)
    hdfs.parse_fsck(moved, {"172.20.0.9:9866", "172.20.0.4:9866"})


@pytest.mark.parametrize(
    "placement",
    [
        fsck(),
        fsck(block_line(locations=("172.20.0.3:9866",))),
        fsck(block_line(locations=("172.20.0.3:9866", "172.20.0.3:9866"))),
        fsck(block_line(), block_line(1, ("172.20.0.3:9866",))),
        fsck(block_line(), count=2),
        fsck(block_line(), block_line()),
        fsck(block_line()).replace("replication=2", "replication=1"),
        fsck(block_line()).replace("Missing blocks: 0", "Missing blocks: 1"),
        fsck(block_line()).replace("Corrupt blocks: 0", "Corrupt blocks: 1"),
        fsck(block_line()).replace("Missing replicas: 0", "Missing replicas: 1"),
        fsck(block_line()).replace("Status: HEALTHY", "Status: CORRUPT"),
        fsck(block_line()).replace("Live_repl=2", "Live_repl=1"),
        fsck(block_line()).replace("len=32768", "len=0"),
    ],
)
def test_zero_partial_duplicate_missing_and_corrupt_placement_is_rejected(placement: str) -> None:
    with pytest.raises(hdfs.VerificationError):
        hdfs.parse_fsck(placement, ADDRESSES)


def test_uri_must_target_the_inspected_cluster() -> None:
    hdfs.validate_cluster_uri("hdfs://hdfs-namenode:8020", "hdfs://hdfs-namenode:8020")
    for uri in ("hdfs://other:8020", "hdfs://hdfs-namenode:9000", "hdfs://hdfs-namenode:8020/x"):
        with pytest.raises(hdfs.VerificationError, match="inspected T01"):
            hdfs.validate_cluster_uri(uri, "hdfs://hdfs-namenode:8020")


def resolved_compose() -> dict:
    return {
        "name": "warehouse-bigdata",
        "networks": {"warehouse-bigdata": {"name": "warehouse-bigdata"}},
        "volumes": {
            f"{service}-data": {"name": f"warehouse-bigdata_{service}-data"}
            for service in hdfs.SERVICES
        },
        "services": {
            service: {
                "image": "apache/hadoop:example@sha256:resolved-by-compose",
                "platform": "linux/amd64",
                "user": "1000:100",
                "hostname": service,
                "networks": {"warehouse-bigdata": {}},
                "volumes": [
                    {
                        "type": "bind",
                        "source": f"/repository/infrastructure/hadoop/{name}",
                        "target": f"/opt/hadoop/etc/hadoop/{name}",
                        "read_only": True,
                    }
                    for name in ("core-site.xml", "hdfs-site.xml")
                ]
                + [
                    {
                        "type": "volume",
                        "source": f"{service}-data",
                        "target": "/var/lib/hdfs/name"
                        if service == "hdfs-namenode"
                        else "/var/lib/hdfs/data",
                    }
                ],
            }
            for service in hdfs.SERVICES
        },
    }


def test_client_spec_comes_from_resolved_compose_and_requires_read_only_xml() -> None:
    config = resolved_compose()
    spec = hdfs.compose_spec(config)
    assert spec.image == config["services"]["hdfs-namenode"]["image"]
    assert (spec.platform, spec.user, spec.network) == (
        "linux/amd64",
        "1000:100",
        "warehouse-bigdata",
    )
    assert len(spec.xml_mounts) == 2
    config["services"]["hdfs-datanode-2"]["volumes"][0]["read_only"] = False
    with pytest.raises(hdfs.VerificationError, match="XML"):
        hdfs.compose_spec(config)


def test_client_removal_requires_both_run_label_and_exact_name() -> None:
    info = {"Name": "/t03-client-owned", "Config": {"Labels": {hdfs.RUN_LABEL: "owned"}}}
    hdfs.assert_owned_client(info, "owned", "t03-client-owned")
    for run_id, name in (("different", "t03-client-owned"), ("owned", "other-client")):
        with pytest.raises(hdfs.VerificationError, match="ownership"):
            hdfs.assert_owned_client(info, run_id, name)


def test_persistence_checks_storage_blocks_content_and_container_replacement() -> None:
    before = {
        "containers": {service: f"old-{service}" for service in hdfs.SERVICES},
        "volumes": {service: {"name": f"volume-{service}"} for service in hdfs.SERVICES},
        "identities": {"clusterID": "CID-a", "uuid1": "u1", "uuid2": "u2"},
        "block_ids": ["BP-1:blk_1_1"],
        "sha256": "content-hash",
        "bytes": 65536,
    }
    hdfs.assert_persistent(before, before, recreated=False)
    after = {**before, "containers": {service: f"new-{service}" for service in hdfs.SERVICES}}
    hdfs.assert_persistent(before, after, recreated=True)
    with pytest.raises(hdfs.VerificationError):
        hdfs.assert_persistent(before, before, recreated=True)
    for key, changed in (
        ("volumes", {}),
        ("identities", {"clusterID": "CID-b"}),
        ("block_ids", ["BP-1:blk_2_1"]),
        ("sha256", "changed-hash"),
        ("bytes", 0),
    ):
        with pytest.raises(hdfs.VerificationError):
            hdfs.assert_persistent(before, {**after, key: changed}, recreated=True)


@pytest.mark.parametrize("path", ["/", "/data/warehouse", DIRECTORY + "/..", DIRECTORY + "/x", ""])
def test_cleanup_rejects_unsafe_paths_before_running_a_command(path: str) -> None:
    calls = []
    with pytest.raises(hdfs.VerificationError):
        fixture = hdfs.FixtureDirectory(path)
        fixture.remove(calls.append)
    assert calls == []


@pytest.mark.parametrize(
    "failure", [RuntimeError("mkdir failed"), subprocess.TimeoutExpired("mkdir", 30)]
)
def test_failed_or_timed_out_mkdir_does_not_grant_cleanup_ownership(failure: Exception) -> None:
    fixture = hdfs.FixtureDirectory(DIRECTORY)
    calls = []

    def fail(path: str) -> None:
        raise failure

    with pytest.raises(type(failure)):
        fixture.create(fail)
    assert not fixture.owned
    with pytest.raises(hdfs.VerificationError, match="ownership"):
        fixture.remove(calls.append)
    assert calls == []


def test_fixture_cleanup_is_exact_and_only_after_exclusive_creation() -> None:
    fixture = hdfs.FixtureDirectory(DIRECTORY)
    created, removed = [], []
    fixture.create(created.append)
    fixture.remove(removed.append)
    assert created == removed == [DIRECTORY]
    with pytest.raises(hdfs.VerificationError, match="ownership"):
        fixture.remove(removed.append)
    assert removed == [DIRECTORY]


def test_cleanup_rejects_a_different_safe_looking_directory() -> None:
    fixture = hdfs.FixtureDirectory(DIRECTORY)
    fixture.create(lambda path: None)
    fixture.path = "/t03-integration-" + "f" * 32
    calls = []
    with pytest.raises(hdfs.VerificationError, match="ownership"):
        fixture.remove(calls.append)
    assert calls == []


@pytest.mark.parametrize(
    "failure", [RuntimeError("stop failed"), subprocess.TimeoutExpired("stop", 300)]
)
def test_restoration_is_registered_before_stop_and_preserves_original_failure(
    failure: Exception,
) -> None:
    calls, errors = [], []
    with pytest.raises(type(failure)) as caught:
        with hdfs.restore_after(lambda: calls.append("restore"), errors):
            calls.append("stop")
            raise failure
    assert caught.value is failure
    assert calls == ["stop", "restore"]
    assert errors == []


def test_restoration_failure_is_separate_from_original_failure() -> None:
    original = RuntimeError("read integrity failure")
    errors = []

    def fail_restore() -> None:
        raise RuntimeError("restore blocked")

    with pytest.raises(RuntimeError) as caught:
        with hdfs.restore_after(fail_restore, errors):
            raise original
    assert caught.value is original
    assert errors == ["Restoration failed: restore blocked"]
    assert "restore blocked" in " ".join(original.__notes__)


def test_restoration_runs_on_success_and_its_failure_fails_the_scenario() -> None:
    calls, errors = [], []
    with hdfs.restore_after(lambda: calls.append("restore"), errors):
        calls.append("read")
    assert calls == ["read", "restore"]

    def fail_restore() -> None:
        raise RuntimeError("start timed out")

    with pytest.raises(RuntimeError, match="start timed out"):
        with hdfs.restore_after(fail_restore, errors):
            pass
    assert errors == ["Restoration failed: start timed out"]


def test_disruptions_require_opt_in_serial_execution_and_two_actual_replicas() -> None:
    nodes = hdfs.parse_report(REPORT)
    blocks = hdfs.parse_fsck(fsck(block_line()), ADDRESSES)
    for enabled, parallel in ((False, False), (True, True)):
        with pytest.raises(hdfs.VerificationError):
            hdfs.require_disruption_prerequisites(enabled, parallel, nodes, blocks)
    with pytest.raises(hdfs.VerificationError):
        hdfs.require_disruption_prerequisites(True, False, nodes, ())
    with pytest.raises(hdfs.VerificationError):
        hdfs.require_disruption_prerequisites(
            True, False, nodes, (hdfs.Block("blk_1", 1, ("172.20.0.3:9866",)),)
        )
    hdfs.require_disruption_prerequisites(True, False, nodes, blocks)


def test_collection_gates_disruptions_before_any_fixture_can_run() -> None:
    marks = []
    item = SimpleNamespace(
        get_closest_marker=lambda name: object() if name == "hdfs_disruptive" else None,
        add_marker=marks.append,
    )
    config = SimpleNamespace(getoption=lambda name, default=None: False)
    pytest_collection_modifyitems(config, [item])
    assert len(marks) == 1
    assert marks[0].name == "skip"
    assert "--run-hdfs-disruptions" in marks[0].kwargs["reason"]


def test_maintenance_window_opt_in_rejects_parallel_collection() -> None:
    item = SimpleNamespace(get_closest_marker=lambda name: object())
    config = SimpleNamespace(
        getoption=lambda name, default=None: True if name == "--run-hdfs-disruptions" else 2
    )
    with pytest.raises(pytest.UsageError, match="serial"):
        pytest_collection_modifyitems(config, [item])


def test_test_body_failure_is_retained_for_fixture_teardown_outcome(tmp_path: Path) -> None:
    # Pytest does not throw test-body exceptions into yield fixtures.
    cluster = object.__new__(hdfs.HDFSVerification)
    cluster.evidence = SimpleNamespace(directory=tmp_path)
    cluster.test_failure = None
    original = RuntimeError("degraded read failed")
    item = SimpleNamespace(funcargs={"hdfs": cluster})
    call = SimpleNamespace(when="call", excinfo=SimpleNamespace(value=original))
    hook = pytest_runtest_makereport(item, call)
    next(hook)
    with pytest.raises(StopIteration):
        next(hook)
    assert cluster.test_failure is original
    assert str(tmp_path) in " ".join(original.__notes__)


def test_scenario_outcome_records_test_failure_and_separate_cleanup_errors(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(hdfs, "ROOT", tmp_path)
    monkeypatch.setattr(hdfs.HDFSVerification, "prepare", lambda self: None)
    original = RuntimeError("fixture content differs")
    with hdfs.verification_run("offline-failed-call", disruptions=False, parallel=False) as cluster:
        cluster.test_failure = original
    outcome = json.loads((cluster.evidence.directory / "outcome.json").read_text())
    assert outcome["status"] == "failed"
    assert "fixture content differs" in outcome["failure"]

    def fail_cleanup(self) -> None:
        self.errors.append("fixture cleanup blocked")

    monkeypatch.setattr(hdfs.HDFSVerification, "cleanup", fail_cleanup)
    with pytest.raises(RuntimeError) as caught:
        with hdfs.verification_run(
            "offline-failed-setup", disruptions=False, parallel=False
        ) as run:
            raise original
    assert caught.value is original
    outcome = json.loads((run.evidence.directory / "outcome.json").read_text())
    assert outcome["status"] == "failed"
    assert outcome["restoration_cleanup_errors"] == ["fixture cleanup blocked"]
    assert "fixture cleanup blocked" in " ".join(original.__notes__)


def test_cluster_lock_rejects_overlapping_runs_before_preflight(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(hdfs, "ROOT", tmp_path)
    calls = []
    monkeypatch.setattr(hdfs.HDFSVerification, "prepare", lambda self: calls.append("prepare"))
    with hdfs.verification_run("offline-lock", disruptions=False, parallel=False):
        with pytest.raises(hdfs.VerificationError, match="cluster lock"):
            with hdfs.verification_run("offline-overlap", disruptions=False, parallel=False):
                pytest.fail("An overlapping run reached its test body.")
    assert calls == ["prepare"]


def test_runner_bounds_timeout_and_retains_raw_evidence(tmp_path: Path, monkeypatch) -> None:
    evidence = hdfs.Evidence(tmp_path, "offline")
    runner = hdfs.Runner(evidence)

    def timeout(args, **kwargs):
        assert kwargs["timeout"] == 30
        assert kwargs["input"] == b"\x00\xff"
        raise subprocess.TimeoutExpired(args, 30, output=b"partial\x00", stderr=b"blocked")

    monkeypatch.setattr(subprocess, "run", timeout)
    with pytest.raises(hdfs.CommandError, match="timed out"):
        runner.run(["docker", "version"], timeout=30, input_data=b"\x00\xff")
    entries = [json.loads(line) for line in evidence.commands_path.read_text().splitlines()]
    assert entries[0]["timed_out"] is True
    assert entries[0]["returncode"] is None
    assert (evidence.directory / entries[0]["stdout"]).read_bytes() == b"partial\x00"
    assert (evidence.directory / entries[0]["stderr"]).read_bytes() == b"blocked"


def test_cleanup_restores_before_removal_and_retains_fixture_if_restore_is_blocked() -> None:
    # Exercise the real cleanup coordinator without creating a client or connecting to Docker.
    run = object.__new__(hdfs.HDFSVerification)
    run.pending_services = hdfs.SERVICES
    run.fixture = hdfs.FixtureDirectory(DIRECTORY)
    run.fixture.create(lambda path: None)
    run.errors = []
    calls = []

    def restore() -> None:
        calls.append("restore")
        raise RuntimeError("Docker unavailable")

    run.restore = restore
    run.remove_fixture = lambda: calls.append("remove fixture")
    run.remove_client = lambda: calls.append("remove client")
    run.cleanup()
    assert calls == ["restore", "remove client"]
    assert any("Docker unavailable" in error for error in run.errors)
    assert any(DIRECTORY in error for error in run.errors)


def test_successful_cleanup_restores_before_fixture_and_client_removal() -> None:
    run = object.__new__(hdfs.HDFSVerification)
    run.pending_services = hdfs.SERVICES
    run.fixture = hdfs.FixtureDirectory(DIRECTORY)
    run.fixture.create(lambda path: None)
    run.errors = []
    calls = []

    def restore() -> None:
        calls.append("restore")
        run.pending_services = ()

    run.restore = restore
    run.remove_fixture = lambda: calls.append("remove fixture")
    run.remove_client = lambda: calls.append("remove client")
    run.cleanup()
    assert calls == ["restore", "remove fixture", "remove client"]
    assert run.errors == []


def test_storage_properties_reject_duplicate_or_missing_identity() -> None:
    required = {"clusterID", "datanodeUuid"}
    assert hdfs.read_properties(
        "# generated\nclusterID=CID-a\ndatanodeUuid=uuid-a\n", required
    ) == {
        "clusterID": "CID-a",
        "datanodeUuid": "uuid-a",
    }
    for text in ("clusterID=CID-a\n", "clusterID=CID-a\nclusterID=CID-b\ndatanodeUuid=u\n"):
        with pytest.raises(hdfs.VerificationError):
            hdfs.read_properties(text, required)


def test_recovery_requires_new_contact_and_block_report_not_cached_live_count() -> None:
    node = hdfs.parse_report(REPORT)["hdfs-datanode-2"]
    threshold = hdfs.report_timestamp("Mon Oct 19 10:00:01 UTC 2026")
    assert not hdfs.has_renewed_contact(node, threshold)
    fresh = REPORT.replace("Mon Oct 19 10:00:00 UTC 2026", "Mon Oct 19 10:00:02 UTC 2026")
    assert hdfs.has_renewed_contact(hdfs.parse_report(fresh)["hdfs-datanode-2"], threshold)


def test_retained_spark_fixture_is_not_deleted_and_fails_outcome(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(hdfs, "ROOT", tmp_path)
    monkeypatch.setattr(hdfs.HDFSVerification, "prepare", lambda self: None)
    removed = []
    monkeypatch.setattr(
        hdfs.HDFSVerification, "remove_fixture", lambda self: removed.append("fixture")
    )
    monkeypatch.setattr(
        hdfs.HDFSVerification, "remove_client", lambda self: removed.append("client")
    )
    with pytest.raises(hdfs.VerificationError, match="retained"):
        with hdfs.verification_run(
            "offline-spark-retain", disruptions=False, parallel=False
        ) as run:
            run.fixture.create(lambda path: None)
            run.retain_fixture("Spark application termination is unproven")
    assert removed == ["client"]
    outcome = json.loads((run.evidence.directory / "outcome.json").read_text())
    assert outcome["status"] == "failed"
    assert outcome["fixture_remaining"]
    assert outcome["retention_reasons"] == ["Spark application termination is unproven"]
    assert run.fixture.path in " ".join(outcome["restoration_cleanup_errors"])


def test_retention_cannot_grant_ownership_or_hide_the_original_failure(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(hdfs, "ROOT", tmp_path)
    monkeypatch.setattr(hdfs.HDFSVerification, "prepare", lambda self: None)
    original = RuntimeError("Spark task evidence missing")
    with pytest.raises(RuntimeError) as caught:
        with hdfs.verification_run(
            "offline-spark-original", disruptions=False, parallel=False
        ) as run:
            run.retain_fixture("Event log retrieval incomplete")
            raise original
    assert caught.value is original
    assert not run.fixture.owned
    outcome = json.loads((run.evidence.directory / "outcome.json").read_text())
    assert outcome["status"] == "failed"
    assert "Spark task evidence missing" in outcome["failure"]
    assert outcome["retention_reasons"] == ["Event log retrieval incomplete"]
