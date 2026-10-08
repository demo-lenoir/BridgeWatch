#!/usr/bin/env python3
"""Run Phase 1 checks against an isolated PostgreSQL 18 instance."""

import os
import pathlib
import shutil
import socket
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]


def run(args, env=None, quiet=False):
    stdout = subprocess.DEVNULL if args[0] == 'pg_ctl' else subprocess.PIPE
    result = subprocess.run(args, cwd=ROOT, env=env, text=True, stdout=stdout, stderr=subprocess.PIPE)
    if result.returncode:
        raise RuntimeError(f"{' '.join(args)} failed:\n{result.stdout or ''}{result.stderr}")
    if not quiet:
        print((result.stdout or '').strip() or f"{args[0]}: OK", flush=True)
    return result.stdout or ''


def source_guards():
    sources = [path for package in ('domain','protocol','reconcile','store') for path in (ROOT / 'internal' / package).rglob('*.go') if not path.name.endswith('_test.go')]
    blocked = ('eth_getLogs', 'eth_subscribe', 'SubscribeFilterLogs', 'FilterLogs', 'websocket')
    for path in sources:
        content = path.read_text()
        if any(marker in content for marker in blocked):
            raise RuntimeError(f'watcher operation found in {path.relative_to(ROOT)}')
    print('Phase 1 direct-ingestion watcher boundary: OK', flush=True)


def main():
    for command in ('go', 'initdb', 'pg_ctl', 'psql', 'ruby'):
        if not shutil.which(command):
            raise RuntimeError(f'missing required command: {command}')
    run(['make', 'verify-phase0'])
    source_guards()
    go_files = [str(path) for path in (ROOT / 'internal').rglob('*.go')]
    if run(['gofmt', '-l', *go_files], quiet=True).strip():
        raise RuntimeError('Go sources need formatting')
    run(['go', 'mod', 'verify'])
    run(['go', 'vet', './...'])
    with tempfile.TemporaryDirectory(prefix='bridgewatch-phase1-') as directory:
        base = pathlib.Path(directory)
        data, socket_dir = base / 'data', base / 'socket'
        socket_dir.mkdir()
        run(['initdb', '-D', str(data), '-A', 'trust', '-U', 'bridgewatch', '--no-instructions'], quiet=True)
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        options = f"-c listen_addresses='' -c unix_socket_directories={socket_dir} -p {port}"
        run(['pg_ctl', '-D', str(data), '-o', options, '-w', 'start'], quiet=True)
        try:
            psql = ['psql', '-X', '-v', 'ON_ERROR_STOP=1', '-h', str(socket_dir), '-p', str(port), '-U', 'bridgewatch', '-d', 'postgres']
            for name in ('0001_initial.up.sql', '0002_phase1_core.up.sql'):
                run([*psql, '-f', str(ROOT / 'migrations' / name)], quiet=True)
            indexes = run([*psql, '-Atc', "SELECT count(*) FROM pg_indexes WHERE schemaname='public' AND indexname IN ('chain_blocks_one_canonical_height','message_anomalies_one_open_episode')"], quiet=True).strip()
            uniques = run([*psql, '-Atc', "SELECT count(*) FROM pg_constraint WHERE contype='u' AND conrelid IN ('message_observations'::regclass,'cross_chain_messages'::regclass)"], quiet=True).strip()
            if indexes != '2' or uniques != '2':
                raise RuntimeError(f'expected key indexes and uniqueness, found {indexes}/{uniques}')
            print('PostgreSQL 18 schema, constraints, and key indexes: OK', flush=True)
            env = os.environ.copy()
            env['BRIDGEWATCH_TEST_DATABASE_URL'] = f'host={socket_dir} port={port} user=bridgewatch dbname=postgres sslmode=disable'
            run(['go', 'test', './...'], env=env)
            run(['go', 'test', '-race', './...'], env=env)
            run(['go', 'test', './internal/protocol', '-run', '^$', '-fuzz', '^FuzzDecodeSource$', '-fuzztime=100x', '-parallel=1'], env=env)
            run(['go', 'test', './internal/protocol', '-run', '^$', '-fuzz', '^FuzzDecodeDestination$', '-fuzztime=100x', '-parallel=1'], env=env)
            for name in ('0002_phase1_core.down.sql', '0001_initial.down.sql'):
                run([*psql, '-f', str(ROOT / 'migrations' / name)], quiet=True)
            print('PostgreSQL migrations up/down: OK', flush=True)
        finally:
            run(['pg_ctl', '-D', str(data), '-m', 'immediate', '-w', 'stop'], quiet=True)
    print('verify-phase1: OK', flush=True)


if __name__ == '__main__':
    main()
