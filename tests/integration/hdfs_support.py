"""Bounded, fixture-scoped verification of the existing T01 HDFS cluster."""

import fcntl
import hashlib
import json
import re
import shlex
import subprocess
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from xml.etree import ElementTree

from warehouse_bigdata.common.config import load_hdfs_config

ROOT = Path(__file__).resolve().parents[2]
SERVICES = ("hdfs-namenode", "hdfs-datanode-1", "hdfs-datanode-2")
DATANODES = SERVICES[1:]
RUN_LABEL = "warehouse-bigdata.t03.run"
XML_TARGETS = tuple(f"/opt/hadoop/etc/hadoop/{name}-site.xml" for name in ("core", "hdfs"))
PAYLOAD = bytes(range(256)) * 256
DIAGNOSTIC_TIMEOUT = 30
IO_TIMEOUT = 120
READY_TIMEOUT = 180
LIFECYCLE_TIMEOUT = 300


class VerificationError(RuntimeError):
    """A failed prerequisite, storage assertion, or restoration/cleanup action."""


class CommandError(VerificationError):
    """An external command failed; raw output is retained in the run evidence."""


def require(condition: object, message: str) -> None:
    if not condition:
        raise VerificationError(message)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class Evidence:
    def __init__(self, root: Path, scenario: str) -> None:
        self.run_id = f"t03-{datetime.now(UTC):%Y%m%dT%H%M%S}-{scenario}-{uuid.uuid4().hex}"
        self.directory = root / self.run_id
        self.directory.mkdir(parents=True, exist_ok=False)
        self.commands_path = self.directory / "commands.jsonl"
        self.sequence = 0

    def write(self, name: str, value: object) -> None:
        (self.directory / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n")

    def event(self, name: str, **details: object) -> None:
        with (self.directory / "events.jsonl").open("a") as stream:
            stream.write(json.dumps({"timestamp": utc_now(), "event": name, **details}) + "\n")


class Runner:
    def __init__(self, evidence: Evidence) -> None:
        self.evidence = evidence

    def run(
        self,
        args: list[str],
        *,
        timeout: float = DIAGNOSTIC_TIMEOUT,
        input_data: bytes | None = None,
        check: bool = True,
    ) -> subprocess.CompletedProcess:
        require(timeout > 0, "Command deadline expired before execution.")
        self.evidence.sequence += 1
        prefix = f"{self.evidence.sequence:04d}"
        entry = {"args": args, "started": utc_now(), "timeout_seconds": timeout}
        self.evidence.event("command-start", **entry)
        timed_out = False
        problem = ""
        try:
            result = subprocess.run(
                args,
                cwd=ROOT,
                input=input_data,
                capture_output=True,
                timeout=timeout,
                check=False,
            )
            stdout, stderr, returncode = result.stdout, result.stderr, result.returncode
        except subprocess.TimeoutExpired as error:
            stdout, stderr, returncode = error.stdout or b"", error.stderr or b"", None
            timed_out = True
            problem = "Command timed out"
        except OSError as error:
            stdout, stderr, returncode = b"", str(error).encode(), None
            problem = "Prerequisite command unavailable"
        output_name, error_name = f"{prefix}.stdout", f"{prefix}.stderr"
        (self.evidence.directory / output_name).write_bytes(stdout)
        (self.evidence.directory / error_name).write_bytes(stderr)
        with self.evidence.commands_path.open("a") as stream:
            stream.write(
                json.dumps(
                    {
                        **entry,
                        "finished": utc_now(),
                        "returncode": returncode,
                        "timed_out": timed_out,
                        "stdout": output_name,
                        "stderr": error_name,
                    }
                )
                + "\n"
            )
        if problem or (check and returncode != 0):
            detail = stderr.decode(errors="replace")[-1500:]
            raise CommandError(
                f"{problem or f'Command failed (exit {returncode})'}: {shlex.join(args)}\n"
                f"{detail}\nEvidence: {self.evidence.directory}"
            )
        return subprocess.CompletedProcess(args, returncode, stdout, stderr)


@dataclass(frozen=True)
class LiveNode:
    hostname: str
    address: str
    last_contact: str
    last_block_report: str


@dataclass(frozen=True)
class Block:
    block_id: str
    length: int
    locations: tuple[str, ...]


def parse_report(text: str) -> dict[str, LiveNode]:
    live = re.search(r"(?m)^Live datanodes \((\d+)\):\s*$", text)
    require(
        live is not None and live[1] == "2", "Expected exactly two live DataNode registrations."
    )
    section = re.split(
        r"(?m)^(?:Dead|Decommissioning|Entering maintenance|In maintenance) datanodes",
        text[live.end() :],
    )[0]
    records = re.split(r"(?m)^Name:\s*", section)[1:]
    nodes = {}
    for record in records:
        fields = dict(re.findall(r"(?m)^([^:\n]+?)\s*:\s*([^\n]*)$", record))
        fields = {key.strip(): value.strip() for key, value in fields.items()}
        host = fields.get("Hostname", "")
        address = record.splitlines()[0].split()[0]
        require(
            host in DATANODES and host not in nodes, f"Unexpected/duplicate live hostname: {host}"
        )
        require(address.endswith(":9866"), f"Unexpected transfer address: {address}")
        require(fields.get("Decommission Status") == "Normal", f"Non-normal registration: {host}")
        nodes[host] = LiveNode(
            host, address, fields.get("Last contact", ""), fields.get("Last Block Report", "")
        )
    require(set(nodes) == set(DATANODES), "Both expected DataNodes must be live.")
    require(len({node.address for node in nodes.values()}) == 2, "Duplicate transfer addresses.")
    return nodes


def parse_fsck(text: str, addresses: set[str]) -> tuple[Block, ...]:
    require(len(addresses) == 2, "Two distinct expected transfer addresses are required.")
    require(re.search(r"(?m)^Status:\s+HEALTHY\s*$", text), "Fixture fsck is not HEALTHY.")
    require(not re.search(r"\b(?:MISSING|CORRUPT)\b", text), "Missing/corrupt fixture blocks.")
    for count in re.findall(
        r"(?im)^\s*(?:Missing blocks|Missing replicas|Corrupt blocks|Blocks with corrupt replicas|"
        r"Under-replicated blocks|Mis-replicated blocks)\s*:\s*(\d+)",
        text,
    ):
        require(
            int(count) == 0, "Fixture fsck reports missing, corrupt, or incomplete replication."
        )
    declared = re.findall(r"replication=(\d+),\s*(\d+) block\(s\):", text)
    require(
        len(declared) == 1 and declared[0][0] == "2",
        "Expected one replicated fixture, replication 2.",
    )
    pattern = (
        r"(?m)^\s*\d+\.\s+((?:BP-[^\s:]+:)?blk_\d+_\d+)\s+len=(\d+)"
        r"\s+Live_repl=(\d+)\s+\[(.*)\]\s*$"
    )
    storage = r"DatanodeInfoWithStorage\[([^,\]]+),[^,\]]+,[^\]]+\]"
    blocks = []
    for block_id, length, replicas, raw_locations in re.findall(pattern, text):
        locations = tuple(re.findall(storage, raw_locations))
        require(not re.sub(storage, "", raw_locations).strip(" ,"), "Unrecognized block locations.")
        require(
            int(replicas) == len(locations) == len(set(locations)) == 2
            and set(locations) == addresses,
            f"Block {block_id} does not have exactly two distinct expected replicas.",
        )
        require(int(length) > 0, f"Empty block: {block_id}")
        blocks.append(Block(block_id, int(length), locations))
    require(
        len(blocks) == int(declared[0][1]) > 0
        and len({block.block_id for block in blocks}) == len(blocks),
        "Expected non-zero, complete, distinct block records for every fixture block.",
    )
    return tuple(blocks)


def read_properties(text: str, required: set[str]) -> dict[str, str]:
    values = {}
    for line in text.splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        require(separator and key not in values and value, "Malformed/duplicate storage identity.")
        values[key] = value
    require(
        required <= values.keys(), f"Missing storage identity fields: {required - values.keys()}"
    )
    return values


def validate_cluster_uri(uri: str, inspected_uri: str) -> None:
    supplied, expected = urlsplit(uri), urlsplit(inspected_uri)
    require(
        (supplied.scheme, supplied.hostname, supplied.port)
        == (expected.scheme, expected.hostname, expected.port)
        and supplied.path in ("", "/"),
        "WAREHOUSE_HDFS_URI must target the inspected T01 cluster's NameNode RPC endpoint.",
    )


@dataclass(frozen=True)
class ComposeSpec:
    image: str
    platform: str
    user: str
    network: str
    xml_mounts: tuple[tuple[str, str], ...]
    volumes: dict[str, str]


def storage_path(service: str) -> str:
    return "/var/lib/hdfs/name" if service == SERVICES[0] else "/var/lib/hdfs/data"


def compose_spec(config: dict) -> ComposeSpec:
    try:
        require(config["name"] == "warehouse-bigdata", "Unexpected Compose project.")
        first = config["services"][SERVICES[0]]
        image, platform, user = first["image"], first["platform"], first["user"]
        require("@sha256:" in image and user == "1000:100", "Unexpected T01 image/client identity.")
        networks = list(first["networks"])
        require(len(networks) == 1, "Expected the single T01 bridge network.")
        network = config["networks"][networks[0]]["name"]
        require(network == "warehouse-bigdata", "Unexpected T01 network.")
        xml_mounts = None
        volumes = {}
        for service in SERVICES:
            entry = config["services"][service]
            require(
                (entry["image"], entry["platform"], entry["user"]) == (image, platform, user)
                and list(entry["networks"]) == networks
                and entry["hostname"] == service,
                f"Inconsistent T01 service contract: {service}",
            )
            mounts = {mount["target"]: mount for mount in entry["volumes"]}
            selected = []
            for target in XML_TARGETS:
                mount = mounts[target]
                require(
                    mount["type"] == "bind" and mount.get("read_only"),
                    "Client XML must be read-only binds.",
                )
                selected.append((mount["source"], target))
            require(
                xml_mounts is None or xml_mounts == tuple(selected),
                "Inconsistent service XML binds.",
            )
            xml_mounts = tuple(selected)
            data = mounts[storage_path(service)]
            require(data["type"] == "volume", "HDFS storage must use existing named volumes.")
            volumes[service] = config["volumes"][data["source"]]["name"]
        require(len(set(volumes.values())) == 3, "HDFS daemons must have distinct named volumes.")
        return ComposeSpec(image, platform, user, network, xml_mounts, volumes)
    except (KeyError, TypeError) as error:
        raise VerificationError(
            f"Incomplete resolved T01 Compose configuration: {error}"
        ) from error


class FixtureDirectory:
    def __init__(self, path: str) -> None:
        require(
            re.fullmatch(r"/t03-integration-[0-9a-f]{32}", path), "Unsafe fixture cleanup path."
        )
        self.path = path
        self.owned = False
        self.attempted = False
        self.created_path: str | None = None

    def create(self, mkdir: Callable[[str], object]) -> None:
        require(not self.attempted, "Fixture creation must be exclusive and attempted only once.")
        self.attempted = True
        mkdir(self.path)  # No -p: ownership is granted only by a successful exclusive mkdir.
        self.created_path = self.path
        self.owned = True

    def remove(self, remove: Callable[[str], object]) -> None:
        require(
            self.owned and self.created_path == self.path,
            "Fixture cleanup refused: exclusive creation ownership is unproven.",
        )
        require(
            re.fullmatch(r"/t03-integration-[0-9a-f]{32}", self.path),
            "Unsafe fixture cleanup path.",
        )
        remove(self.path)
        self.owned = False


def assert_owned_client(info: dict, run_id: str, name: str) -> None:
    require(
        info.get("Name") == f"/{name}"
        and info.get("Config", {}).get("Labels", {}).get(RUN_LABEL) == run_id,
        "Client removal refused: run label/name ownership mismatch.",
    )


def disruption_blocker(enabled: bool, parallel: bool) -> str | None:
    if not enabled:
        return "Requires --run-hdfs-disruptions in an exclusive maintenance window."
    if parallel:
        return "HDFS disruptions require serial pytest execution (no xdist workers)."
    return None


def require_disruption_prerequisites(
    enabled: bool, parallel: bool, nodes: Mapping[str, LiveNode], blocks: tuple[Block, ...]
) -> None:
    blocker = disruption_blocker(enabled, parallel)
    require(blocker is None, blocker or "")
    require(set(nodes) == set(DATANODES), "Both DataNodes must be registered before disruption.")
    addresses = {node.address for node in nodes.values()}
    require(
        blocks
        and len(addresses) == 2
        and all(
            len(block.locations) == 2 and set(block.locations) == addresses for block in blocks
        ),
        "Disruption requires confirmed two-node placement of every fixture block.",
    )


@contextmanager
def restore_after(restore: Callable[[], None], failures: list[str]) -> Iterator[None]:
    """Enter before any lifecycle command, including commands that may time out."""
    original = None
    try:
        yield
    except BaseException as error:
        original = error
        raise
    finally:
        try:
            restore()
        except BaseException as error:
            message = f"Restoration failed: {error}"
            failures.append(message)
            if original is None:
                raise
            original.add_note(message)


def assert_persistent(before: dict, after: dict, *, recreated: bool) -> None:
    for field in ("volumes", "identities", "block_ids", "sha256", "bytes"):
        require(before[field] == after[field], f"Persistence changed {field}.")
    for service in SERVICES:
        replaced = before["containers"][service] != after["containers"][service]
        require(replaced == recreated, f"Unexpected container replacement state: {service}")


def report_timestamp(value: str) -> float:
    try:
        return datetime.strptime(value, "%a %b %d %H:%M:%S %Z %Y").replace(tzinfo=UTC).timestamp()
    except ValueError as error:
        raise VerificationError(f"Unusable UTC DataNode report timestamp: {value!r}") from error


def has_renewed_contact(node: LiveNode, since: float) -> bool:
    return (
        report_timestamp(node.last_contact) >= since
        and report_timestamp(node.last_block_report) >= since
    )


def remaining(deadline: float) -> float:
    seconds = deadline - time.monotonic()
    require(seconds > 0, "Readiness/replication polling deadline expired.")
    return min(DIAGNOSTIC_TIMEOUT, seconds)


class HDFSVerification:
    def __init__(self, evidence: Evidence, *, disruptions: bool, parallel: bool) -> None:
        self.evidence = evidence
        self.runner = Runner(evidence)
        self.disruptions = disruptions
        self.parallel = parallel
        self.fixture = FixtureDirectory(f"/t03-integration-{uuid.uuid4().hex}")
        self.file = f"{self.fixture.path}/payload"
        self.client = f"t03-client-{uuid.uuid4().hex}"
        self.client_attempted = False
        self.pending_services: tuple[str, ...] = ()
        self.errors: list[str] = []
        self.test_failure: BaseException | None = None
        self.retention_reasons: list[str] = []

    def compose(
        self, *args: str, timeout: float = DIAGNOSTIC_TIMEOUT
    ) -> subprocess.CompletedProcess:
        return self.runner.run(
            ["docker", "compose", "-f", str(ROOT / "infrastructure/compose.yaml"), *args],
            timeout=timeout,
        )

    def daemon(self, service: str, *args: str, timeout: float = DIAGNOSTIC_TIMEOUT) -> str:
        return self.compose(
            "exec",
            "-T",
            service,
            "timeout",
            "--kill-after=5s",
            f"{timeout:g}s",
            *args,
            timeout=timeout + 10,
        ).stdout.decode()

    def client_command(
        self, *args: str, timeout: float = DIAGNOSTIC_TIMEOUT, input_data: bytes | None = None
    ) -> bytes:
        return self.runner.run(
            [
                "docker",
                "exec",
                "-i",
                self.client,
                "timeout",
                "--kill-after=5s",
                f"{timeout:g}s",
                *args,
            ],
            timeout=timeout + 10,
            input_data=input_data,
        ).stdout

    def hdfs(
        self, *args: str, timeout: float = DIAGNOSTIC_TIMEOUT, input_data: bytes | None = None
    ) -> str:
        return self.client_command("hdfs", *args, timeout=timeout, input_data=input_data).decode()

    def wait_ready(self) -> None:
        self.daemon(
            SERVICES[0],
            "bash",
            "/opt/warehouse-hdfs/healthcheck.sh",
            "wait-ready",
            str(READY_TIMEOUT),
            timeout=READY_TIMEOUT + 5,
        )

    def inspect_daemons(self) -> dict[str, dict]:
        ids = self.compose("ps", "--all", "--quiet", *SERVICES).stdout.decode().split()
        require(
            len(ids) == 3, "HDFS prerequisite: start the three existing initialized T01 daemons."
        )
        inspected = json.loads(self.runner.run(["docker", "inspect", *ids]).stdout)
        daemons = {}
        for info in inspected:
            labels = info["Config"]["Labels"]
            service = labels.get("com.docker.compose.service")
            require(
                labels.get("com.docker.compose.project") == "warehouse-bigdata"
                and service in SERVICES
                and service not in daemons,
                "Unexpected/duplicate inspected HDFS daemon ownership.",
            )
            state = info["State"]
            require(
                state["Running"] and not state["Paused"] and not state["Restarting"],
                f"HDFS prerequisite: {service} is not running normally; no automatic startup.",
            )
            require(
                info["Config"]["Image"] == self.spec.image
                and info["Config"]["User"] == self.spec.user
                and info["Config"]["Hostname"] == service
                and self.spec.network in info["NetworkSettings"]["Networks"],
                f"Running daemon differs from resolved T01 configuration: {service}",
            )
            mounts = {mount["Destination"]: mount for mount in info["Mounts"]}
            for source, target in self.spec.xml_mounts:
                require(
                    mounts[target]["Type"] == "bind"
                    and mounts[target]["Source"] == source
                    and not mounts[target]["RW"],
                    f"Running daemon XML differs from Compose: {service}",
                )
            data = mounts[storage_path(service)]
            require(
                data["Type"] == "volume"
                and data["Name"] == self.spec.volumes[service]
                and data["RW"],
                f"Unexpected persistent volume: {service}",
            )
            daemons[service] = info
        require(set(daemons) == set(SERVICES), "Missing inspected HDFS daemon.")
        require(len({info["Image"] for info in daemons.values()}) == 1, "HDFS image IDs differ.")
        return daemons

    def storage_snapshot(self) -> dict:
        daemons = self.inspect_daemons()
        identities = {}
        for service in SERVICES:
            required = {"clusterID", "storageType", "layoutVersion", "cTime"}
            required |= (
                {"namespaceID", "blockpoolID"} if service == SERVICES[0] else {"datanodeUuid"}
            )
            identities[service] = read_properties(
                self.daemon(service, "cat", f"{storage_path(service)}/current/VERSION"), required
            )
            expected_type = "NAME_NODE" if service == SERVICES[0] else "DATA_NODE"
            require(identities[service]["storageType"] == expected_type, "Unexpected storage type.")
        require(
            len({value["clusterID"] for value in identities.values()}) == 1, "Cluster IDs differ."
        )
        require(
            len({identities[service]["datanodeUuid"] for service in DATANODES}) == 2,
            "DataNode storage UUIDs must be distinct.",
        )
        snapshot = {
            "containers": {service: info["Id"] for service, info in daemons.items()},
            "volumes": {
                service: {"name": self.spec.volumes[service], "target": storage_path(service)}
                for service in SERVICES
            },
            "identities": identities,
            "image_ids": {service: info["Image"] for service, info in daemons.items()},
        }
        self.evidence.event("storage-identities", **snapshot)
        return snapshot

    def create_client(self) -> None:
        args = [
            "docker",
            "create",
            "--name",
            self.client,
            "--label",
            f"{RUN_LABEL}={self.evidence.run_id}",
            "--platform",
            self.spec.platform,
            "--network",
            self.spec.network,
            "--user",
            self.spec.user,
            "--memory",
            "512m",
            "--cpus",
            "1",
            "--env",
            "HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop",
            "--env",
            "HADOOP_HEAPSIZE_MIN=128",
            "--env",
            "HADOOP_HEAPSIZE_MAX=256",
            "--env",
            "TZ=UTC",
            "--entrypoint",
            "/bin/bash",
        ]
        for source, target in self.spec.xml_mounts:
            args.extend(["--mount", f"type=bind,source={source},target={target},readonly"])
        args.extend([self.spec.image, "-c", "exec sleep infinity"])
        self.client_attempted = True  # Label-checked cleanup also covers create/start timeouts.
        self.runner.run(args)
        self.runner.run(["docker", "start", self.client])
        info = json.loads(self.runner.run(["docker", "inspect", self.client]).stdout)[0]
        assert_owned_client(info, self.evidence.run_id, self.client)
        require(
            len(info["Mounts"]) == 2
            and all(mount["Type"] == "bind" and not mount["RW"] for mount in info["Mounts"]),
            "Temporary client must mount only the two read-only XML files, without storage volumes.",
        )
        self.evidence.write("client", info)

    def prepare(self) -> None:
        try:
            self.config = load_hdfs_config()
            self.evidence.write("configuration", asdict(self.config))
            resolved = json.loads(self.compose("config", "--format", "json").stdout)
            self.spec = compose_spec(resolved)
            self.evidence.write("client-spec", asdict(self.spec))
            xml = {}
            for source, _ in self.spec.xml_mounts:
                for prop in ElementTree.fromstring(Path(source).read_bytes()).findall("property"):
                    name, value = prop.findtext("name"), prop.findtext("value")
                    require(
                        name and value and name not in xml,
                        "Malformed/duplicate client XML property.",
                    )
                    xml[name] = value.strip()
            require(
                xml.get("fs.defaultFS") == "hdfs://hdfs-namenode:8020", "Unexpected T01 defaultFS."
            )
            validate_cluster_uri(self.config.uri, xml["fs.defaultFS"])
            require(
                xml.get("dfs.replication") == "2",
                "Configured replication must be 2; no setrep override.",
            )
            require(
                xml.get("dfs.namenode.acls.enabled") == "true",
                "HDFS ACL authorization must be enabled for scoped Spark permissions.",
            )
            require(
                xml.get("dfs.namenode.rpc-address") == "hdfs-namenode:8020",
                "Unexpected RPC address.",
            )
            self.storage_snapshot()
            self.evidence.write(
                "image",
                json.loads(self.runner.run(["docker", "image", "inspect", self.spec.image]).stdout),
            )
            self.runner.run(["docker", "network", "inspect", self.spec.network])
            self.wait_ready()
            self.create_client()
            self.client_command(
                "bash",
                "-ec",
                "for host in hdfs-namenode hdfs-datanode-1 hdfs-datanode-2; "
                'do getent hosts "$host"; done; '
                "for endpoint in hdfs-namenode:8020 hdfs-datanode-1:9866 hdfs-datanode-2:9866; do "
                "host=${endpoint%:*}; port=${endpoint##*:}; "
                "timeout --kill-after=1s 3s bash -c "
                '\'exec 3<>/dev/tcp/$1/$2\' _ "$host" "$port"; done',
            )
            for key, expected in (
                ("fs.defaultFS", xml["fs.defaultFS"]),
                ("dfs.replication", "2"),
                ("dfs.namenode.acls.enabled", "true"),
                ("dfs.client.use.datanode.hostname", "true"),
                ("dfs.datanode.use.datanode.hostname", "true"),
            ):
                require(
                    self.hdfs("getconf", "-confKey", key).strip() == expected,
                    f"Client configuration mismatch: {key}",
                )
            require(
                self.daemon(SERVICES[0], "hdfs", "getconf", "-confKey", "dfs.replication").strip()
                == "2",
                "NameNode configured replication must be 2.",
            )
            self.hdfs("dfs", "-ls", "/")
            self.live_nodes()
            require(
                self.hdfs("dfsadmin", "-safemode", "get").strip() == "Safe mode is OFF",
                "NameNode safe mode is not off.",
            )
        except Exception as error:
            raise VerificationError(f"HDFS prerequisite blocked: {error}") from error
        self.evidence.event(
            "fixture-create", path=self.fixture.path, base_path=self.config.base_path
        )
        self.fixture.create(lambda path: self.hdfs("dfs", "-mkdir", path))
        self.hdfs("dfs", "-put", "-", self.file, timeout=IO_TIMEOUT, input_data=PAYLOAD)
        require(
            self.hdfs("dfs", "-stat", "%r %b", self.file).strip() == f"2 {len(PAYLOAD)}",
            "Fixture replication/size must be 2/65536; no setrep override.",
        )

    def live_nodes(self, *, timeout: float = DIAGNOSTIC_TIMEOUT) -> dict[str, LiveNode]:
        nodes = parse_report(self.hdfs("dfsadmin", "-report", timeout=timeout))
        self.evidence.event(
            "live-registration", nodes={host: asdict(node) for host, node in nodes.items()}
        )
        return nodes

    def placement(self) -> tuple[dict[str, LiveNode], tuple[Block, ...]]:
        deadline = time.monotonic() + READY_TIMEOUT
        last_error = ""
        while time.monotonic() < deadline:
            try:
                nodes = self.live_nodes(timeout=remaining(deadline))
                blocks = parse_fsck(
                    self.hdfs(
                        "fsck",
                        self.file,
                        "-files",
                        "-blocks",
                        "-locations",
                        timeout=remaining(deadline),
                    ),
                    {node.address for node in nodes.values()},
                )
                require(
                    sum(block.length for block in blocks) == len(PAYLOAD),
                    "Fixture block lengths differ from the payload.",
                )
                return nodes, blocks
            except VerificationError as error:
                last_error = str(error)
            time.sleep(min(2, max(0, deadline - time.monotonic())))
        raise VerificationError(
            f"Two-replica fixture placement not ready within {READY_TIMEOUT}s: {last_error}"
        )

    def read_fixture(self, label: str) -> dict:
        # Each exec creates a fresh JVM: no pre-opened stream or client block cache.
        actual = self.client_command("hdfs", "dfs", "-cat", self.file, timeout=IO_TIMEOUT)
        result = {"bytes": len(actual), "sha256": hashlib.sha256(actual).hexdigest()}
        self.evidence.write(
            label, {**result, "expected_sha256": hashlib.sha256(PAYLOAD).hexdigest()}
        )
        require(actual == PAYLOAD, f"Independent-client read integrity failed: {label}")
        return result

    def verify_fixture(self, label: str) -> dict:
        nodes, blocks = self.placement()
        content = self.read_fixture(f"{label}-content")
        snapshot = {
            **self.storage_snapshot(),
            **content,
            "block_ids": sorted(block.block_id for block in blocks),
            "placement": [asdict(block) for block in blocks],
            "nodes": {host: asdict(node) for host, node in nodes.items()},
        }
        self.evidence.write(label, snapshot)
        return snapshot

    def restore(self) -> None:
        if not self.pending_services:
            return
        self.compose(
            "up",
            "-d",
            "--no-deps",
            "--no-recreate",
            "--wait",
            "--wait-timeout",
            str(READY_TIMEOUT),
            *self.pending_services,
            timeout=LIFECYCLE_TIMEOUT,
        )
        self.wait_ready()
        self.inspect_daemons()
        self.pending_services = ()
        self.evidence.event("daemons-restored")

    def check_persistence(self, *, recreate: bool) -> None:
        before = self.verify_fixture("baseline")
        nodes, blocks = self.placement()
        require_disruption_prerequisites(self.disruptions, self.parallel, nodes, blocks)
        self.inspect_daemons()
        self.pending_services = SERVICES
        with restore_after(self.restore, self.errors):
            if recreate:
                self.compose(
                    "up",
                    "-d",
                    "--no-deps",
                    "--force-recreate",
                    "--wait",
                    "--wait-timeout",
                    str(READY_TIMEOUT),
                    *SERVICES,
                    timeout=LIFECYCLE_TIMEOUT,
                )
            else:
                self.compose(
                    "restart", "--no-deps", "--timeout", "60", *SERVICES, timeout=LIFECYCLE_TIMEOUT
                )
            self.wait_ready()
            after = self.verify_fixture("after-recreation" if recreate else "after-restart")
            assert_persistent(before, after, recreated=recreate)

    def assert_stopped(self, container_id: str) -> None:
        info = json.loads(self.runner.run(["docker", "inspect", container_id]).stdout)[0]
        self.evidence.event(
            "interrupted-container-state", container_id=container_id, state=info["State"]
        )
        require(
            not info["State"]["Running"] and info["State"]["Status"] == "exited",
            "Interrupted DataNode is not confirmed stopped.",
        )

    def check_interruption(self) -> None:
        before = self.verify_fixture("baseline")
        # Refresh actual placement immediately before registering restoration and stopping DN2.
        nodes, blocks = self.placement()
        require_disruption_prerequisites(self.disruptions, self.parallel, nodes, blocks)
        self.inspect_daemons()
        self.pending_services = (DATANODES[1],)
        with restore_after(self.restore, self.errors):
            self.compose("stop", "--timeout", "60", DATANODES[1], timeout=LIFECYCLE_TIMEOUT)
            self.assert_stopped(before["containers"][DATANODES[1]])
            # This raw report may still list two live nodes for about 630 seconds.
            self.hdfs("dfsadmin", "-report")
            self.read_fixture("degraded-read")
            self.assert_stopped(before["containers"][DATANODES[1]])
        # Trigger a new full report; stale NameNode live counts cannot satisfy recovery.
        since = int(time.time())
        self.hdfs("dfsadmin", "-triggerBlockReport", f"{DATANODES[1]}:9867")
        deadline = time.monotonic() + READY_TIMEOUT
        while time.monotonic() < deadline:
            nodes = self.live_nodes(timeout=remaining(deadline))
            if has_renewed_contact(nodes[DATANODES[1]], since):
                break
            time.sleep(min(2, max(0, deadline - time.monotonic())))
        else:
            raise VerificationError(
                "Restored DataNode has no renewed contact/full block-report evidence."
            )
        self.evidence.event(
            "renewed-contact-and-block-report", since=since, node=asdict(nodes[DATANODES[1]])
        )
        after = self.verify_fixture("after-recovery")
        assert_persistent(before, after, recreated=False)

    def retain_fixture(self, reason: str) -> None:
        require(isinstance(reason, str) and reason.strip(), "Fixture retention needs a reason.")
        if reason not in self.retention_reasons:
            self.retention_reasons.append(reason)
            self.errors.append(f"Fixture retained at {self.fixture.path}: {reason}")
            self.evidence.event("fixture-retained", path=self.fixture.path, reason=reason)

    def remove_fixture(self) -> None:
        require(not self.retention_reasons, "Fixture cleanup refused: retained for investigation.")
        self.fixture.remove(
            lambda path: self.hdfs("dfs", "-rm", "-r", "-skipTrash", path, timeout=IO_TIMEOUT)
        )

    def remove_client(self) -> None:
        if not self.client_attempted:
            return
        inspected = self.runner.run(["docker", "inspect", self.client], check=False)
        if inspected.returncode != 0:
            require(
                b"No such object" in inspected.stderr or b"No such container" in inspected.stderr,
                "Cannot establish client ownership for cleanup.",
            )
            return
        info = json.loads(inspected.stdout)[0]
        assert_owned_client(info, self.evidence.run_id, self.client)
        self.runner.run(["docker", "rm", "--force", self.client])

    def cleanup(self) -> None:
        if self.pending_services:
            try:
                self.restore()
            except Exception as error:
                self.errors.append(f"Restoration failed during cleanup: {error}")
        if self.fixture.owned:
            if getattr(self, "retention_reasons", []):
                pass  # Retention was recorded as a failure when requested; never delete this path.
            elif self.pending_services:
                self.errors.append(
                    f"Fixture retained until daemons are restored: {self.fixture.path}"
                )
            else:
                try:
                    self.remove_fixture()
                except Exception as error:
                    self.errors.append(
                        f"Fixture cleanup failed; remaining path {self.fixture.path}: {error}"
                    )
        elif self.fixture.attempted:
            self.errors.append(
                "Unproven mkdir ownership; inspect possible remaining path without deleting it: "
                f"{self.fixture.path}"
            )
        try:
            self.remove_client()
        except Exception as error:
            self.errors.append(f"Client cleanup failed; remaining client {self.client}: {error}")


@contextmanager
def verification_run(
    scenario: str, *, disruptions: bool, parallel: bool
) -> Iterator[HDFSVerification]:
    evidence = Evidence(ROOT / "artifacts/test-runs", scenario)
    verification = HDFSVerification(evidence, disruptions=disruptions, parallel=parallel)
    original = None
    started = utc_now()
    evidence.event(
        "scenario-start",
        scenario=scenario,
        fixture=verification.fixture.path,
        client=verification.client,
    )
    # Serialize all T03 runs, including baseline reads concurrent with a disruption.
    with (evidence.directory.parent / ".hdfs-verification.lock").open("a") as lock:
        try:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise VerificationError(
                    "HDFS prerequisite: another T03 verification run holds the cluster lock."
                ) from error
            verification.prepare()
            yield verification
        except BaseException as error:
            original = error
            error.add_note(f"T03 evidence: {evidence.directory}")
            raise
        finally:
            verification.cleanup()
            failure = original or verification.test_failure
            try:
                evidence.write(
                    "outcome",
                    {
                        "scenario": scenario,
                        "started": started,
                        "finished": utc_now(),
                        "status": "failed"
                        if failure is not None or verification.errors
                        else "passed",
                        "failure": repr(failure) if failure is not None else None,
                        "restoration_cleanup_errors": verification.errors,
                        "fixture": verification.fixture.path,
                        "fixture_remaining": verification.fixture.owned,
                        "retention_reasons": verification.retention_reasons,
                        "fixture_creation_uncertain": verification.fixture.attempted
                        and verification.fixture.created_path is None,
                        "client": verification.client,
                    },
                )
            except Exception as error:
                verification.errors.append(f"Outcome evidence write failed: {error}")
            if verification.errors:
                message = "\n".join(verification.errors)
                if original is not None:
                    original.add_note(message)
                else:
                    raise VerificationError(f"{message}\nT03 evidence: {evidence.directory}")
