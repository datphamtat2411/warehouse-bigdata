"""Storage-safety checks for T01, without Docker or a Hadoop installation."""

import json
import os
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

import pytest

ROOT = Path(__file__).resolve().parents[2]
LIFECYCLE = ROOT / "infrastructure/hadoop/lifecycle.sh"
INITIALIZER = ROOT / "scripts/init-hdfs.sh"
CLUSTER_ID = "CID-11111111-1111-1111-1111-111111111111"


def namenode_metadata(directory: Path) -> None:
    current = directory / "current"
    current.mkdir()
    (current / "VERSION").write_text(
        f"storageType=NAME_NODE\nclusterID={CLUSTER_ID}\nnamespaceID=123\n"
        "blockpoolID=BP-123-127.0.0.1-123\ncTime=0\nlayoutVersion=-66\n"
    )
    (current / "seen_txid").write_text("0\n")
    image = current / "fsimage_0000000000000000000"
    image.write_bytes(b"synthetic image, not a real Hadoop fixture")
    image.with_suffix(".md5").write_text(f"{'0' * 32} *{image.name}\n")


def datanode_metadata(directory: Path, cluster_id: str = CLUSTER_ID) -> None:
    current = directory / "current"
    current.mkdir()
    (current / "VERSION").write_text(
        f"storageType=DATA_NODE\nclusterID={cluster_id}\n"
        "cTime=0\nlayoutVersion=-57\nstorageID=DS-123\ndatanodeUuid=uuid-123\n"
    )


def snapshot(directories: list[Path]) -> dict[str, bytes | None]:
    return {
        str(path): path.read_bytes() if path.is_file() else None
        for directory in directories
        for path in directory.rglob("*")
    }


@pytest.fixture
def sandbox(tmp_path: Path) -> dict:
    directories = [tmp_path / name for name in ("name", "data-1", "data-2")]
    for directory in directories:
        directory.mkdir()
    binaries = tmp_path / "bin"
    binaries.mkdir()
    log = tmp_path / "commands.jsonl"
    # Every external mutating operation is observable; no root privileges are used.
    stub = f"#!{sys.executable}\n" + dedent(
        """\
        import json
        import os
        import sys
        from pathlib import Path

        name = Path(sys.argv[0]).name
        args = sys.argv[1:]
        with open(os.environ["TEST_COMMAND_LOG"], "a") as stream:
            stream.write(json.dumps([name, *args]) + "\\n")
        if name == "id":
            print(os.environ.get("TEST_UID", "1000"))
        elif name == "runuser":
            assert args[:3] == ["-u", "hadoop", "--"]
            os.execvp(args[3], args[3:])
        elif name == "hdfs" and "-format" in args:
            current = Path(os.environ["HDFS_NAMENODE_DIR"]) / "current"
            current.mkdir()
            (current / "format-sentinel").write_text("formatted once")
            if os.environ.get("TEST_FORMAT_FAIL") == "1":
                sys.exit(1)
        elif name == "docker":
            if "ps" in args:
                print("\\n".join(json.loads(os.environ.get("TEST_DOCKER_STATES", "{}"))))
            elif args[0] == "inspect":
                print(json.loads(os.environ["TEST_DOCKER_STATES"])[args[-1]])
        """
    )
    for name in ("id", "chown", "runuser", "hdfs", "docker"):
        executable = binaries / name
        executable.write_text(stub)
        executable.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{binaries}:{os.environ['PATH']}",
        "TEST_COMMAND_LOG": str(log),
        "TEST_UID": "0",
        "HDFS_NAMENODE_DIR": str(directories[0]),
        "HDFS_DATANODE_1_DIR": str(directories[1]),
        "HDFS_DATANODE_2_DIR": str(directories[2]),
        "HDFS_DATANODE_DIR": str(directories[1]),
        "HDFS_DATANODE_HOSTNAME": "hdfs-datanode-1",
    }
    return {"directories": directories, "env": env, "log": log, "cwd": tmp_path}


def invoke(sandbox: dict, mode: str, *, initializer: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(INITIALIZER)] if initializer else ["bash", str(LIFECYCLE), mode],
        env=sandbox["env"],
        cwd=sandbox["cwd"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def commands(sandbox: dict, name: str) -> list[list[str]]:
    log = sandbox["log"]
    entries = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
    return [entry for entry in entries if entry[0] == name]


def test_fresh_initialization_formats_once_as_hadoop(sandbox: dict) -> None:
    result = invoke(sandbox, "init")
    assert result.returncode == 0, result.stderr
    assert commands(sandbox, "hdfs") == [["hdfs", "namenode", "-format", "-nonInteractive"]]
    assert commands(sandbox, "runuser") == [
        ["runuser", "-u", "hadoop", "--", "hdfs", "namenode", "-format", "-nonInteractive"]
    ]
    assert commands(sandbox, "chown") == [
        ["chown", "1000:100", str(directory)] for directory in sandbox["directories"]
    ]


@pytest.mark.parametrize("with_datanodes", [False, True])
def test_reinitialization_is_non_mutating(sandbox: dict, with_datanodes: bool) -> None:
    name, data1, data2 = sandbox["directories"]
    namenode_metadata(name)
    if with_datanodes:
        for directory in (data1, data2):
            datanode_metadata(directory)
            (directory / ".sentinel").write_bytes(b"retain blocks and identity")
    (name / ".sentinel").write_bytes(b"retain namespace")
    before = snapshot(sandbox["directories"])
    for _ in range(2):
        result = invoke(sandbox, "init")
        assert result.returncode == 0, result.stderr
        assert "already initialized" in result.stdout.lower()
    assert snapshot(sandbox["directories"]) == before
    assert not commands(sandbox, "hdfs")
    assert not commands(sandbox, "chown")
    assert not commands(sandbox, "runuser")


@pytest.mark.parametrize(
    "state",
    [
        "hidden-name",
        "hidden-data",
        "partial-name",
        "missing-image",
        "missing-checksum",
        "malformed-name-version",
        "duplicate-cluster-id",
        "orphaned-data",
        "unrecognized-data",
        "malformed-data-version",
        "mismatched-cluster-id",
    ],
)
def test_inconsistent_storage_is_preserved_and_never_formatted(sandbox: dict, state: str) -> None:
    name, data1, data2 = sandbox["directories"]
    if state == "hidden-name":
        (name / ".retained").write_bytes(b"not empty")
    elif state == "hidden-data":
        (data2 / ".retained").write_bytes(b"not empty")
    elif state == "partial-name":
        (name / "current").mkdir()
        (name / "current/VERSION").write_text(f"clusterID={CLUSTER_ID}\n")
    elif state == "orphaned-data":
        datanode_metadata(data1)
    else:
        namenode_metadata(name)
        if state == "missing-image":
            (name / "current/fsimage_0000000000000000000").unlink()
        elif state == "missing-checksum":
            (name / "current/fsimage_0000000000000000000.md5").unlink()
        elif state == "malformed-name-version":
            (name / "current/VERSION").write_text("not storage metadata\n")
        elif state == "duplicate-cluster-id":
            with (name / "current/VERSION").open("a") as stream:
                stream.write(f"clusterID={CLUSTER_ID}\n")
        elif state == "unrecognized-data":
            (data1 / "retained-block").write_bytes(b"do not delete")
        elif state == "malformed-data-version":
            datanode_metadata(data1)
            (data1 / "current/VERSION").write_text("storageType=DATA_NODE\n")
        elif state == "mismatched-cluster-id":
            datanode_metadata(data2, "CID-another-cluster")
    before = snapshot(sandbox["directories"])
    result = invoke(sandbox, "init")
    assert result.returncode != 0
    assert snapshot(sandbox["directories"]) == before
    assert not commands(sandbox, "hdfs")
    assert not commands(sandbox, "chown")
    assert not commands(sandbox, "runuser")


def test_failed_format_is_not_automatically_retried(sandbox: dict) -> None:
    sandbox["env"]["TEST_FORMAT_FAIL"] = "1"
    assert invoke(sandbox, "init").returncode != 0
    before = snapshot(sandbox["directories"])
    assert invoke(sandbox, "init").returncode != 0
    assert snapshot(sandbox["directories"]) == before
    assert len(commands(sandbox, "hdfs")) == 1


@pytest.mark.parametrize("mode", ["namenode", "datanode"])
def test_ordinary_startup_only_executes_the_daemon(sandbox: dict, mode: str) -> None:
    name, data1, _ = sandbox["directories"]
    namenode_metadata(name)
    datanode_metadata(data1)
    sandbox["env"]["TEST_UID"] = "1000"
    before = snapshot(sandbox["directories"])
    result = invoke(sandbox, mode)
    assert result.returncode == 0, result.stderr
    assert commands(sandbox, "hdfs") == [["hdfs", mode]]
    assert not commands(sandbox, "chown")
    assert not commands(sandbox, "runuser")
    assert snapshot(sandbox["directories"]) == before


def test_empty_datanode_can_register_normally(sandbox: dict) -> None:
    sandbox["env"]["TEST_UID"] = "1000"
    result = invoke(sandbox, "datanode")
    assert result.returncode == 0, result.stderr
    assert commands(sandbox, "hdfs") == [["hdfs", "datanode"]]


@pytest.mark.parametrize("mode", ["namenode", "datanode"])
def test_uninitialized_or_unrecognized_storage_never_triggers_startup_format(
    sandbox: dict, mode: str
) -> None:
    sandbox["env"]["TEST_UID"] = "1000"
    if mode == "datanode":
        (sandbox["directories"][1] / ".retained").write_bytes(b"unknown blocks")
    before = snapshot(sandbox["directories"])
    assert invoke(sandbox, mode).returncode != 0
    assert not commands(sandbox, "hdfs")
    assert not commands(sandbox, "chown")
    assert snapshot(sandbox["directories"]) == before


@pytest.mark.parametrize("state", ["running", "paused", "restarting", "removing"])
def test_initializer_refuses_active_daemons(sandbox: dict, state: str) -> None:
    sandbox["env"]["TEST_DOCKER_STATES"] = json.dumps({"container-id": state})
    result = invoke(sandbox, "", initializer=True)
    assert result.returncode != 0
    assert not any("run" in command for command in commands(sandbox, "docker"))


@pytest.mark.parametrize("states", [{}, {"container-id": "exited"}, {"container-id": "created"}])
def test_initializer_resolves_paths_and_only_runs_explicit_helper(
    sandbox: dict, states: dict
) -> None:
    sandbox["env"]["TEST_DOCKER_STATES"] = json.dumps(states)
    result = invoke(sandbox, "", initializer=True)
    assert result.returncode == 0, result.stderr
    docker = commands(sandbox, "docker")
    runs = [command for command in docker if "run" in command]
    assert len(runs) == 1
    run = runs[0]
    assert run[run.index("-f") + 1] == str(ROOT / "infrastructure/compose.yaml")
    assert run[run.index("--profile") + 1] == "init"
    assert run[-5:] == ["run", "--rm", "--no-deps", "hdfs-init", "init"]
