#!/usr/bin/env python3
"""Build and validate unsigned local release evidence for the exact HEAD."""

import hashlib
import json
import os
import pathlib
import platform
import shutil
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "out" / "phase5"
RELEASE = OUTPUT / "release"


def run(args, env=None, stdout=None):
    result = subprocess.run(args, cwd=ROOT, env=env, text=True, stdout=stdout or subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        raise RuntimeError(f"{args[0]} failed (exit {result.returncode}): {result.stderr or result.stdout}")
    return result.stdout or ""


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def json_stream(path):
    content = path.read_text()
    decoder = json.JSONDecoder()
    position = 0
    while position < len(content):
        position += len(content[position:]) - len(content[position:].lstrip())
        if position >= len(content):
            break
        value, position = decoder.raw_decode(content, position)
        yield value


def scan_results(path):
    document = json.loads(path.read_text())
    return {
        "vulnerabilities": [item["VulnerabilityID"] for result in document.get("Results", []) for item in result.get("Vulnerabilities") or []],
        "secrets": sum(len(result.get("Secrets") or []) for result in document.get("Results", [])),
    }


def main():
    if run(["git", "status", "--porcelain"]).strip():
        raise RuntimeError("commit source and documentation before building release evidence")
    source = run(["git", "rev-parse", "HEAD"]).strip()
    if len(source) != 40:
        raise RuntimeError("invalid source commit")
    for command in ("docker", "syft", "trivy", "go"):
        if not shutil.which(command):
            raise RuntimeError(f"missing command: {command}")
    RELEASE.mkdir(parents=True, exist_ok=True)
    versions = {
        "go": run(["go", "version"]).strip(),
        "docker": run(["docker", "version", "--format", "{{.Client.Version}} {{.Server.Version}}"]).strip(),
        "syft": run(["syft", "version"]).strip().splitlines()[:2],
        "trivy": run(["trivy", "--version"]).strip().splitlines()[:2],
    }
    build_commands = []
    artifacts = {}
    for architecture in ("amd64", "arm64"):
        name = f"bridgewatch-linux-{architecture}"
        target = RELEASE / name
        args = ["go", "build", "-trimpath", "-buildvcs=false", "-ldflags=-s -w -buildid=", "-o", str(target), "./cmd/bridgewatch"]
        env = os.environ.copy()
        env.update({"GOOS": "linux", "GOARCH": architecture, "CGO_ENABLED": "0"})
        run(args, env=env)
        build_commands.append(f"GOOS=linux GOARCH={architecture} CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w -buildid=' ./cmd/bridgewatch")
        artifacts[name] = digest(target)
    tag = f"bridgewatch:local-{source[:12]}"
    run(["docker", "build", "-t", tag, "."])
    image = json.loads(run(["docker", "image", "inspect", tag]))[0]
    if image["Config"]["User"] != "65532:65532":
        raise RuntimeError("container runtime user is not the configured non-root identity")
    archive = RELEASE / "bridgewatch-image.tar"
    run(["docker", "save", "-o", str(archive), tag])
    artifacts[archive.name] = digest(archive)
    build_commands.append(f"docker build -t {tag} .; docker save {tag}")
    sbom = RELEASE / "container-sbom.spdx.json"
    run(["syft", f"docker-archive:{archive}", "-o", f"spdx-json={sbom}"])
    document = json.loads(sbom.read_text())
    if document.get("spdxVersion") != "SPDX-2.3" or len(document.get("packages", [])) < 2 or not document.get("relationships"):
        raise RuntimeError("container SBOM structure is incomplete")
    artifacts[sbom.name] = digest(sbom)

    scanner = OUTPUT / "tools" / "govulncheck"
    scanner.parent.mkdir(parents=True, exist_ok=True)
    if not scanner.exists():
        env = os.environ.copy()
        env["GOBIN"] = str(scanner.parent)
        run(["go", "install", "golang.org/x/vuln/cmd/govulncheck@v1.8.0"], env=env)
    versions["govulncheck"] = run([str(scanner), "-version"]).strip().splitlines()[:2]
    vuln_file = RELEASE / "govulncheck.json"
    with vuln_file.open("w") as output:
        run([str(scanner), "-json", "./..."], stdout=output)
    go_findings = sorted(set(item["finding"]["osv"] for item in json_stream(vuln_file) if "finding" in item))
    fs_file = RELEASE / "trivy-fs.json"
    run(["trivy", "fs", "--scanners", "vuln,secret", "--format", "json", "--output", str(fs_file), "--skip-dirs", "out", "--skip-dirs", ".git", "."])
    image_file = RELEASE / "trivy-image.json"
    run(["trivy", "image", "--input", str(archive), "--scanners", "vuln,secret", "--format", "json", "--output", str(image_file)])
    fs_results, image_results = scan_results(fs_file), scan_results(image_file)
    scans = {"govulncheck_findings": go_findings, "trivy_source": fs_results, "trivy_image": image_results, "tool_versions": versions, "disposition": "no unresolved scanner findings"}
    (RELEASE / "scan-summary.json").write_text(json.dumps(scans, indent=2) + "\n")
    if go_findings or fs_results["vulnerabilities"] or fs_results["secrets"] or image_results["vulnerabilities"] or image_results["secrets"]:
        raise RuntimeError("scanner findings remain; inspect release/scan-summary.json")

    checksums = RELEASE / "SHA256SUMS"
    checksums.write_text("".join(f"{value}  {name}\n" for name, value in sorted(artifacts.items())))
    provenance = {
        "kind": "local unsigned provenance",
        "source_commit": source,
        "source_tree": run(["git", "rev-parse", "HEAD^{tree}"]).strip(),
        "build_commands": build_commands,
        "environment": {"host_os": platform.system(), "host_architecture": platform.machine(), "versions": versions},
        "image_id": image["Id"],
        "image_runtime_user": image["Config"]["User"],
        "artifacts_sha256": artifacts,
        "sbom_artifact": sbom.name,
        "sbom_sha256": artifacts[sbom.name],
        "scan_summary_sha256": digest(RELEASE / "scan-summary.json"),
    }
    path = RELEASE / "provenance.json"
    path.write_text(json.dumps(provenance, indent=2) + "\n")
    saved = json.loads(path.read_text())
    if saved["source_commit"] != run(["git", "rev-parse", "HEAD"]).strip():
        raise RuntimeError("provenance source commit differs from HEAD")
    for name, value in saved["artifacts_sha256"].items():
        if digest(RELEASE / name) != value:
            raise RuntimeError(f"artifact digest mismatch: {name}")
    if saved["sbom_sha256"] != digest(sbom) or saved["scan_summary_sha256"] != digest(RELEASE / "scan-summary.json"):
        raise RuntimeError("evidence digest mismatch")
    print(f"local release: OK source={source} artifacts={RELEASE}")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"local release: FAILED: {error}", file=sys.stderr)
        sys.exit(1)
