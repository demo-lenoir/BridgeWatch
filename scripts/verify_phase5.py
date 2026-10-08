#!/usr/bin/env python3
"""Complete local Phase 5 gate for a committed source tree."""

import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]


def run(args):
    subprocess.run(args, cwd=ROOT, check=True)


def main():
    run(["make", "verify-phase4"])
    run(["make", "demo"])
    run(["make", "benchmark-smoke"])
    run(["make", "benchmark"])
    run(["python3", "scripts/clean_clone_verify.py"])
    run(["python3", "scripts/release_phase5.py"])
    print("verify-phase5: OK", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"verify-phase5: FAILED: {error}", file=sys.stderr)
        sys.exit(1)
