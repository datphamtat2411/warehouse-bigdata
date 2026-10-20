"""Stream into an approved runtime: bounded Spark ZSTD JNI load, no Spark context."""

import argparse
import json
import os
import signal
import stat
import subprocess
import tempfile
from pathlib import Path

NATIVE = "/opt/warehouse-spark/native"
PROBE_ROOT = Path("/tmp")
JARS = "/opt/spark/jars/*"
JAVA_SOURCE = r"""
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import org.apache.spark.SparkConf;
import org.apache.spark.io.ZStdCompressionCodec;
import com.github.luben.zstd.util.Native;

public class WarehouseZstdProbe {
    public static void main(String[] args) throws Exception {
        System.out.println("java_tmpdir=" + System.getProperty("java.io.tmpdir"));
        System.out.println("java_version=" + System.getProperty("java.version"));
        System.out.println("java_user=" + System.getProperty("user.name"));
        byte[] original = new byte[65536];
        new Random(42).nextBytes(original);
        ZStdCompressionCodec codec = new ZStdCompressionCodec(new SparkConf(false));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (OutputStream encoded = codec.compressedOutputStream(bytes)) {
            encoded.write(original);
        }
        byte[] decoded;
        try (InputStream input = codec.compressedInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            decoded = input.readAllBytes();
        }
        if (!Arrays.equals(original, decoded)) {
            throw new IllegalStateException("ZSTD round trip changed the payload");
        }
        System.out.println("codec_round_trip=true");
        System.out.println("native_loaded=" + Native.isLoaded());
        for (String mapping : Files.readAllLines(Path.of("/proc/self/maps"))) {
            if (mapping.contains("/libzstd-jni-") && mapping.split("\\s+")[1].contains("x")) {
                System.out.println("native_mapping=" + mapping);
            }
        }
    }
}
"""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def parse_mountinfo(text: str, target: str) -> tuple[str, set[str]]:
    """Effective per-mount flags control executable mappings, not superblock flags."""
    for line in text.splitlines():
        mount, filesystem = line.split(" - ", 1)
        fields = mount.split()
        if fields[4] == target:
            return filesystem.split()[0], set(fields[5].split(","))
    raise ValueError(f"Missing exact mount for {target}")


def collect_runtime() -> dict:
    mountinfo = Path("/proc/self/mountinfo").read_text()
    mounts = {}
    for target in ("/", "/tmp", NATIVE):
        filesystem, options = parse_mountinfo(mountinfo, target)
        path = Path(target)
        identity, usage = path.stat(), os.statvfs(path)
        mounts[target] = {
            "filesystem": filesystem,
            "options": sorted(options),
            "uid": identity.st_uid,
            "gid": identity.st_gid,
            "mode": f"{stat.S_IMODE(identity.st_mode):04o}",
            "capacity": usage.f_blocks * usage.f_frsize,
            "used": (usage.f_blocks - usage.f_bfree) * usage.f_frsize,
        }
    cgroup = Path("/sys/fs/cgroup")
    if (cgroup / "memory.max").exists():
        memory = int((cgroup / "memory.max").read_text())
        quota, period = map(int, (cgroup / "cpu.max").read_text().split())
    else:
        memory = int((cgroup / "memory/memory.limit_in_bytes").read_text())
        quota = int((cgroup / "cpu/cpu.cfs_quota_us").read_text())
        period = int((cgroup / "cpu/cpu.cfs_period_us").read_text())
    return {
        "uid": os.getuid(),
        "gid": os.getgid(),
        "mounts": mounts,
        "memory_limit": memory,
        "cpus": quota / period,
    }


def validate_runtime(runtime: dict, *, bulk_mib: int, memory_mib: int, cpus: float) -> None:
    require(runtime["uid"] == runtime["gid"] == 185, "Expected UID/GID 185:185")
    require(runtime["memory_limit"] == memory_mib * 1024**2, "Cgroup memory limit mismatch")
    require(runtime["cpus"] == cpus, "Cgroup CPU limit mismatch")
    mounts = runtime["mounts"]
    require("ro" in mounts["/"]["options"], "Container root must be read-only")
    for target, size, mode, flags in (
        ("/tmp", bulk_mib, "1777", {"rw", "nosuid", "nodev", "noexec"}),
        (NATIVE, 32, "0700", {"rw", "nosuid", "nodev"}),
    ):
        mount = mounts[target]
        require(mount["filesystem"] == "tmpfs", f"{target}: expected tmpfs")
        require(flags <= set(mount["options"]), f"{target}: incorrect mount flags")
        require(mount["capacity"] == size * 1024**2, f"{target}: incorrect capacity")
        require(mount["mode"] == mode, f"{target}: incorrect permissions")
    private = mounts[NATIVE]
    require("noexec" not in private["options"], f"{NATIVE}: native extraction is noexec")
    require(private["uid"] == private["gid"] == 185, f"{NATIVE}: incorrect ownership")


def run_bounded(argv: list[str], *, timeout: float) -> dict:
    """Kill the invocation-owned process group on deadline, including descendants."""
    with subprocess.Popen(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    ) as process:
        timed_out = False
        try:
            output, error = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            output, error = process.communicate(timeout=2)
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.communicate(timeout=2)
            raise
        return {
            "exit_code": process.returncode,
            "timed_out": timed_out,
            "stdout": output,
            "stderr": error,
        }


def parse_java_output(output: str) -> dict:
    facts = {"native_mappings": []}
    for line in output.splitlines():
        key, separator, value = line.partition("=")
        if separator and key == "native_mapping":
            facts["native_mappings"].append(value)
        elif separator and key in {
            "java_tmpdir",
            "java_version",
            "java_user",
            "codec_round_trip",
            "native_loaded",
        }:
            require(key not in facts, f"Duplicate Java evidence: {key}")
            facts[key] = value
    return facts


def validate_java(result: dict) -> dict:
    require(not result["timed_out"], "Java JNI probe exceeded its deadline")
    require(result["exit_code"] == 0, f"Java JNI probe exited {result['exit_code']}")
    facts = parse_java_output(result["stdout"])
    require(facts.get("java_tmpdir") == NATIVE, "Incorrect effective JVM temporary directory")
    require(facts.get("java_version") == "11.0.30", "Incorrect Java runtime version")
    require(facts.get("java_user") == "spark", "Incorrect JVM user")
    require(facts.get("codec_round_trip") == "true", "Missing successful Spark ZSTD round trip")
    require(facts.get("native_loaded") == "true", "ZSTD JNI did not load")
    require(bool(facts["native_mappings"]), "Missing executable ZSTD JNI mapping")
    for mapping in facts["native_mappings"]:
        fields = mapping.split(maxsplit=5)
        require(len(fields) == 6 and "x" in fields[1], "ZSTD JNI mapping is not executable")
        library = Path(fields[5].removesuffix(" (deleted)"))
        require(
            library.parent == Path(NATIVE) and library.name.startswith("libzstd-jni-"),
            "ZSTD JNI mapping is outside the private mount",
        )
    return facts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bulk-mib", type=int, choices=(96, 224), required=True)
    parser.add_argument("--memory-mib", type=int, required=True)
    parser.add_argument("--cpus", type=float, required=True)
    parser.add_argument(
        "--java-tmpdir",
        choices=("/tmp", NATIVE),
        help="Explicit JVM option for the regression's precedence control only",
    )
    args = parser.parse_args(argv)
    report = {"ok": False}
    try:
        report["runtime"] = collect_runtime()
        validate_runtime(
            report["runtime"], bulk_mib=args.bulk_mib, memory_mib=args.memory_mib, cpus=args.cpus
        )
        # Class files do not require executable storage. Delete only this private workspace;
        # the bundled ZSTD loader registers its own extracted library for deletion on exit.
        with tempfile.TemporaryDirectory(
            prefix="warehouse-zstd-probe-", dir=PROBE_ROOT
        ) as temporary:
            source = Path(temporary) / "WarehouseZstdProbe.java"
            source.write_text(JAVA_SOURCE)
            report["compile"] = run_bounded(
                [
                    "javac",
                    "-J-Xms16m",
                    "-J-Xmx96m",
                    "-J-XX:ActiveProcessorCount=1",
                    "-cp",
                    JARS,
                    "-d",
                    temporary,
                    str(source),
                ],
                timeout=10,
            )
            require(not report["compile"]["timed_out"], "JNI probe compilation timed out")
            require(report["compile"]["exit_code"] == 0, "JNI probe compilation failed")
            command = ["java", "-Xms16m", "-Xmx128m", "-XX:ActiveProcessorCount=1"]
            if args.java_tmpdir is not None:
                command.append(f"-Djava.io.tmpdir={args.java_tmpdir}")
            report["java"] = run_bounded(
                [*command, "-cp", f"{temporary}:{JARS}", "WarehouseZstdProbe"],
                timeout=20,
            )
            report["jvm"] = parse_java_output(report["java"]["stdout"])
            validate_java(report["java"])
            report["ok"] = True
    except (OSError, RuntimeError, ValueError, KeyError) as error:
        report["error"] = str(error)
    print(json.dumps(report, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
