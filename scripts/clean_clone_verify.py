#!/usr/bin/env python3
"""Verify the committed tree without relying on checkout-local files."""

import os
import pathlib
import subprocess
import sys
import tarfile
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]


def run(args, cwd, env=None):
    result = subprocess.run(args, cwd=cwd, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError(f"{' '.join(args)} failed:\n{result.stdout}")
    print(f"{' '.join(args)}: OK", flush=True)


def main():
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip():
        raise RuntimeError("clean-clone verification requires a committed source tree")
    with tempfile.TemporaryDirectory(prefix="bridgewatch-clean-clone-") as directory:
        base = pathlib.Path(directory)
        archive = base / "source.tar"
        with archive.open("wb") as output:
            subprocess.run(["git", "archive", "--format=tar", "HEAD"], cwd=ROOT, stdout=output, check=True)
        checkout = base / "checkout"
        checkout.mkdir()
        with tarfile.open(archive) as bundle:
            for member in bundle.getmembers():
                if member.issym() or member.islnk():
                    raise RuntimeError("archive links are not supported")
                target = (checkout / member.name).resolve()
                if not target.is_relative_to(checkout.resolve()):
                    raise RuntimeError("unsafe archive member")
            bundle.extractall(checkout)
        env = os.environ.copy()
        env.pop("BRIDGEWATCH_FOUNDRY_BIN", None)
        run(["go", "mod", "download"], checkout, env)
        run(["go", "build", "./cmd/bridgewatch"], checkout, env)
        run(["make", "verify-phase4"], checkout, env)
        run(["make", "demo"], checkout, env)
        run(["make", "benchmark-smoke"], checkout, env)
    print("clean-clone-verify: OK", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"clean-clone-verify: FAILED: {error}", file=sys.stderr)
        sys.exit(1)
