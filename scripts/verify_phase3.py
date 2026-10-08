#!/usr/bin/env python3
"""Run two-chain completion checks against isolated PostgreSQL and Foundry."""

import os
import pathlib
import shutil
import socket
import tempfile

from verify_phase2 import ROOT, foundry_tools, phase2_guards, run


def phase3_guards():
    phase2_guards()
    runtime = [p for p in (ROOT / 'internal').rglob('*.go') if not p.name.endswith('_test.go')]
    if any('relayer' in p.parts or p.name.startswith('relayer') for p in runtime):
        raise RuntimeError('relay submission package found')
    required = [ROOT / 'internal' / 'destination' / 'watcher.go',
                ROOT / 'internal' / 'store' / 'canonical_projection.go',
                ROOT / 'migrations' / '0004_destination_watcher.up.sql']
    if any(not p.is_file() for p in required):
        raise RuntimeError('missing destination runtime or migration')
    print('Phase 3 runtime scope: OK', flush=True)


def main():
    for command in ('go', 'initdb', 'pg_ctl', 'psql', 'postgres'):
        if not shutil.which(command):
            raise RuntimeError(f'missing command: {command}')
    forge, anvil, portable = foundry_tools()
    previous = os.environ.get('BRIDGEWATCH_FOUNDRY_BIN')
    os.environ['BRIDGEWATCH_FOUNDRY_BIN'] = str(pathlib.Path(forge).parent)
    try:
        run(['make', 'verify-phase2'])
        phase3_guards()
        print(run(['go', 'version'], quiet=True).strip(), flush=True)
        print(run(['postgres', '--version'], quiet=True).strip(), flush=True)
        print(run([forge, '--version'], quiet=True).splitlines()[0], flush=True)
        print(run([anvil, '--version'], quiet=True).splitlines()[0], flush=True)
        run([forge, 'build'])
        go_files = [str(p) for p in (ROOT / 'internal').rglob('*.go')]
        if run(['gofmt', '-l', *go_files], quiet=True).strip():
            raise RuntimeError('unformatted Go source')
        run(['go', 'mod', 'verify'])
        run(['go', 'vet', './...'])
        with tempfile.TemporaryDirectory(prefix='bridgewatch-phase3-') as directory:
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
                for number in range(1, 6):
                    matches = list((ROOT / 'migrations').glob(f'{number:04d}_*.up.sql'))
                    if len(matches) != 1:
                        raise RuntimeError(f'expected one upward migration {number:04d}')
                    run([*psql, '-f', str(matches[0])], quiet=True)
                tables = run([*psql, '-Atc', "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('source_streams','destination_streams','source_reorgs','destination_reorgs')"], quiet=True).strip()
                if tables != '4':
                    raise RuntimeError(f'expected four watcher tables, found {tables}')
                env = os.environ.copy()
                url = f'host={socket_dir} port={port} user=bridgewatch dbname=postgres sslmode=disable'
                for key in ('BRIDGEWATCH_TEST_DATABASE_URL', 'BRIDGEWATCH_TEST_SOURCE_DB_URL', 'BRIDGEWATCH_TEST_DESTINATION_DB_URL'):
                    env[key] = url
                env['BRIDGEWATCH_ANVIL_BIN'] = anvil
                run(['go', 'test', '-count=1', '-p', '1', './...'], env=env)
                run(['go', 'test', '-race', '-count=1', '-p', '1', './...'], env=env)
                run(['go', 'test', '-count=1', '-v', '-run', '^TestRealTwoAnvilCompletionAndDestinationFirst$', './internal/destination'], env=env)
                run(['go', 'test', './internal/destination', '-run', '^$', '-fuzz', '^FuzzDestinationRange$', '-fuzztime=100x', '-parallel=1'], env=env)
                run(['go', 'test', './internal/destination', '-run', '^$', '-fuzz', '^FuzzDestinationAncestor$', '-fuzztime=100x', '-parallel=1'], env=env)
                for number in range(5, 0, -1):
                    matches = list((ROOT / 'migrations').glob(f'{number:04d}_*.down.sql'))
                    if len(matches) != 1:
                        raise RuntimeError(f'expected one downward migration {number:04d}')
                    run([*psql, '-f', str(matches[0])], quiet=True)
                print('PostgreSQL 18 migrations up/down: OK', flush=True)
            finally:
                run(['pg_ctl', '-D', str(data), '-m', 'immediate', '-w', 'stop'], quiet=True)
        print('verify-phase3: OK', flush=True)
    finally:
        if previous is None:
            os.environ.pop('BRIDGEWATCH_FOUNDRY_BIN', None)
        else:
            os.environ['BRIDGEWATCH_FOUNDRY_BIN'] = previous
        if portable is not None:
            portable.cleanup()


if __name__ == '__main__':
    main()
