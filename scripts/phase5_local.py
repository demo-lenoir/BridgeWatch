#!/usr/bin/env python3
"""Fresh local PostgreSQL harness for the final demo and benchmark."""

import contextlib
import hashlib
import json
import os
import pathlib
import platform
import shutil
import socket
import statistics
import sys
import tempfile

from verify_phase2 import ROOT, foundry_tools, run

OUTPUT = ROOT / "out" / "phase5"
DEMO_CHECKSUM = "79c87f304c3a1eb24633b7c4f31d9e3d8ecd07c2b886f6e1b74a9efa22aa2ec4"


def host_memory():
    if platform.system() == "Darwin":
        return run(["sysctl", "-n", "hw.memsize"], quiet=True).strip()
    path = pathlib.Path("/proc/meminfo")
    if path.exists():
        first = path.read_text().splitlines()[0].split()
        return str(int(first[1]) * 1024)
    return "unavailable"


def host_cpu():
    if platform.system() == "Darwin":
        return run(["sysctl", "-n", "machdep.cpu.brand_string"], quiet=True).strip()
    path = pathlib.Path("/proc/cpuinfo")
    if path.exists():
        for line in path.read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    return platform.processor() or "unavailable"


@contextlib.contextmanager
def database():
    for command in ("initdb", "pg_ctl", "psql", "postgres", "go"):
        if not shutil.which(command):
            raise RuntimeError(f"missing command: {command}")
    version = run(["postgres", "--version"], quiet=True).strip()
    if "18.6" not in version:
        raise RuntimeError(f"PostgreSQL 18.6 required: {version}")
    with tempfile.TemporaryDirectory(prefix="bridgewatch-phase5-") as directory:
        base = pathlib.Path(directory)
        data, socket_dir = base / "data", base / "socket"
        socket_dir.mkdir()
        run(["initdb", "-D", str(data), "-A", "trust", "-U", "bridgewatch", "--no-instructions"], quiet=True)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        options = f"-c listen_addresses='' -c unix_socket_directories={socket_dir} -p {port}"
        run(["pg_ctl", "-D", str(data), "-o", options, "-w", "start"], quiet=True)
        try:
            psql = ["psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", str(socket_dir), "-p", str(port), "-U", "bridgewatch", "-d", "postgres"]
            for number in range(1, 6):
                matches = list((ROOT / "migrations").glob(f"{number:04d}_*.up.sql"))
                if len(matches) != 1:
                    raise RuntimeError(f"expected one migration {number:04d}")
                run([*psql, "-f", str(matches[0])], quiet=True)
            yield f"host={socket_dir} port={port} user=bridgewatch dbname=postgres sslmode=disable"
        finally:
            run(["pg_ctl", "-D", str(data), "-m", "immediate", "-w", "stop"], quiet=True)


def validate_demo(path):
    evidence = json.loads(path.read_text())
    if evidence["checksum"] != DEMO_CHECKSUM:
        raise RuntimeError("demo checksum mismatch")
    values = []
    for prefix, mapping in (("count", evidence["counts"]), ("state", evidence["state_histogram"]), ("anomaly", evidence["anomaly_histogram"])):
        values.extend(f"{prefix}.{key}={value}" for key, value in mapping.items())
    rebuilt = hashlib.sha256("\n".join(sorted(values)).encode()).hexdigest()
    if rebuilt != evidence["checksum"]:
        raise RuntimeError("demo evidence checksum does not match its counts")
    if evidence["counts"] != {"alert_episodes": 1, "anomalies": 4, "completed": 2, "destination_reorgs": 2, "duplicate_business_transitions": 0, "messages": 2, "observations": 8, "open_stuck": 0, "source_reorgs": 1, "transitions": 15}:
        raise RuntimeError("demo evidence counts differ")
    if evidence["state_histogram"] != {"COMPLETED": 2} or evidence["anomaly_histogram"] != {"CONFLICT": 1, "DUPLICATE_EXECUTION": 1, "REORGED": 1, "STUCK": 1}:
        raise RuntimeError("demo evidence histogram differs")
    if "18.6" not in evidence["tool_versions"]["postgresql"] or "1.8.5" not in evidence["tool_versions"]["anvil"]:
        raise RuntimeError("demo did not use pinned local tool versions")
    if evidence["duration_ms"] <= 0:
        raise RuntimeError("demo duration missing")
    return evidence


def validate_benchmark(path, messages):
    evidence = json.loads(path.read_text())
    if not evidence["valid"] or evidence["workload"]["source_messages"] != messages:
        raise RuntimeError("benchmark correctness gate failed")
    if evidence["payload_checksum"] != evidence["expected_payload_checksum"]:
        raise RuntimeError("benchmark payload checksum differs")
    counts = evidence["counts"]
    expected_transitions = 68 if messages == 20 else messages * 69 // 20
    if counts["messages"] != messages or counts["duplicate_business_transitions"] != 0 or counts["observations"] != messages * 39 // 20 or counts["transitions"] != expected_transitions:
        raise RuntimeError("benchmark exact counts differ")
    if evidence["state_histogram"] != {"COMPLETED": messages * 17 // 20, "RELAY_PENDING": messages // 10, "SOURCE_SEEN": messages // 20}:
        raise RuntimeError("benchmark state histogram differs")
    if evidence["anomaly_histogram"] != {"DUPLICATE_EXECUTION": messages // 20, "STUCK": messages // 10}:
        raise RuntimeError("benchmark anomaly histogram differs")
    return evidence


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("demo", "benchmark-smoke", "benchmark"):
        raise RuntimeError("usage: phase5_local.py demo|benchmark-smoke|benchmark")
    mode = sys.argv[1]
    OUTPUT.mkdir(parents=True, exist_ok=True)
    forge, anvil, portable = foundry_tools()
    try:
        anvil_version = run([anvil, "--version"], quiet=True).strip()
        if "1.8.5" not in anvil_version:
            raise RuntimeError("Anvil 1.8.5 required")
        env = os.environ.copy()
        env["BRIDGEWATCH_ANVIL_BIN"] = anvil
        env["BRIDGEWATCH_ANVIL_VERSION"] = anvil_version.splitlines()[0]
        env["BRIDGEWATCH_HOST_CPU"] = host_cpu()
        env["BRIDGEWATCH_HOST_MEMORY_BYTES"] = host_memory()
        if mode == "demo":
            run([forge, "build"])
        with database() as url:
            env["BRIDGEWATCH_TEST_DESTINATION_DB_URL"] = url
            if mode == "demo":
                path = OUTPUT / "demo.json"
                path.unlink(missing_ok=True)
                env["BRIDGEWATCH_DEMO_EVIDENCE"] = str(path)
                run(["go", "test", "-count=1", "-v", "-run", "^TestFinalDemo$", "./internal/destination"], env=env)
                evidence = validate_demo(path)
                print(f"demo: OK checksum={evidence['checksum']} evidence={path}", flush=True)
            else:
                messages, repetitions = (20, 1) if mode == "benchmark-smoke" else (80, 3)
                env["BRIDGEWATCH_BENCH_MESSAGES"] = str(messages)
                results = []
                for repetition in range(1, repetitions + 1):
                    path = OUTPUT / f"benchmark-{messages}-run-{repetition:02d}.json"
                    path.unlink(missing_ok=True)
                    env["BRIDGEWATCH_BENCH_EVIDENCE"] = str(path)
                    try:
                        run(["go", "test", "-count=1", "-run", "^TestPhase5BenchmarkWorkload$", "./internal/destination"], env=env)
                        results.append(validate_benchmark(path, messages))
                    except Exception as error:
                        invalid = path.with_name(path.stem + "-invalid.json")
                        invalid.write_text(json.dumps({"valid": False, "error": str(error)}, indent=2) + "\n")
                        raise
                rates = [result["measurements"]["messages_per_second"] for result in results]
                summary = {"valid_runs": repetitions, "source_messages_per_run": messages, "median_messages_per_second": statistics.median(rates), "all_messages_per_second": rates, "raw_results": [f"benchmark-{messages}-run-{i:02d}.json" for i in range(1, repetitions + 1)], "scope": "local in-process RPC fixture and PostgreSQL; not a production SLA"}
                path = OUTPUT / f"benchmark-{messages}-summary.json"
                path.write_text(json.dumps(summary, indent=2) + "\n")
                print(f"benchmark: OK median={summary['median_messages_per_second']:.2f} messages/s summary={path}", flush=True)
    finally:
        if portable is not None:
            portable.cleanup()


if __name__ == "__main__":
    main()
