#!/usr/bin/env python3
"""Run Phase 4 operational checks on an isolated PostgreSQL 18 cluster."""

import os
import pathlib
import shutil
import socket
import tempfile

from verify_phase2 import ROOT, foundry_tools, phase2_guards, run


def phase4_guards():
    phase2_guards()
    required = (
        'cmd/bridgewatch/main.go', 'internal/api/server.go',
        'internal/operations/service.go', 'internal/operations/outbox.go',
        'internal/operations/webhook.go', 'migrations/0005_operations.up.sql',
    )
    if any(not (ROOT / name).is_file() for name in required):
        raise RuntimeError('Phase 4 runtime artifact missing')
    runtime = [p for p in (ROOT / 'internal').rglob('*.go') if not p.name.endswith('_test.go')]
    if any('relayer' in p.parts or p.name.startswith('relayer') for p in runtime):
        raise RuntimeError('relay submission package found')
    go_files = [str(p) for p in (ROOT / 'cmd').rglob('*.go')]
    go_files += [str(p) for p in (ROOT / 'internal').rglob('*.go')]
    if run(['gofmt', '-l', *go_files], quiet=True).strip():
        raise RuntimeError('unformatted Go source')
    print('Phase 4 runtime, formatting and scope: OK', flush=True)


def main():
    run(['make', 'verify-phase3'])
    phase4_guards()
    for command in ('go', 'initdb', 'pg_ctl', 'psql', 'postgres'):
        if not shutil.which(command):
            raise RuntimeError(f'missing command: {command}')
    version = run(['postgres', '--version'], quiet=True).strip()
    if '18.6' not in version:
        raise RuntimeError(f'PostgreSQL 18.6 required: {version}')
    forge, anvil, portable = foundry_tools()
    try:
        run(['go', 'mod', 'verify'])
        run(['go', 'vet', './...'])
        with tempfile.TemporaryDirectory(prefix='bridgewatch-phase4-') as directory:
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
                    files = list((ROOT / 'migrations').glob(f'{number:04d}_*.up.sql'))
                    if len(files) != 1:
                        raise RuntimeError(f'expected upward migration {number:04d}')
                    run([*psql, '-f', str(files[0])], quiet=True)
                env = os.environ.copy()
                url = f'host={socket_dir} port={port} user=bridgewatch dbname=postgres sslmode=disable'
                for key in ('BRIDGEWATCH_TEST_DATABASE_URL', 'BRIDGEWATCH_TEST_SOURCE_DB_URL', 'BRIDGEWATCH_TEST_DESTINATION_DB_URL'):
                    env[key] = url
                env['BRIDGEWATCH_ANVIL_BIN'] = anvil
                run(['go', 'test', '-count=1', '-p', '1', './...'], env=env)
                run(['go', 'test', '-race', '-count=1', '-p', '1', './...'], env=env)
                run(['go', 'test', '-count=1', '-v', '-run', '^TestRealTwoAnvilCompletionAndDestinationFirst$', './internal/destination'], env=env)
                run(['go', 'test', '-count=1', '-v', '-run', '^Test(ReadOnlyAPIAndShutdown|OperationalSLABoundaryLateDeliveryAndRestart|DestinationOutageSuppressesNewStuck|StuckSourceReorgAndDestinationReorgNewEpisode|StuckSourceReincludedOnNewCanonicalBranchResetsAnchor|TwoStuckWorkersAndCompletionRace|CompletionBeforeSLANoEpisode|AlertResponseLostRetriesSameIdentity|AlertDeliveryOutcomesAndRestart|ConcurrentAlertWorkersClaimOnce|AlertLeaseExpiryAfterWorkerCrash|ManualIncidentAlertIsChainScoped)$', './internal/api', './internal/destination'], env=env)
                for number in range(5, 0, -1):
                    files = list((ROOT / 'migrations').glob(f'{number:04d}_*.down.sql'))
                    if len(files) != 1:
                        raise RuntimeError(f'expected downward migration {number:04d}')
                    run([*psql, '-f', str(files[0])], quiet=True)
                print(f'{version} migrations up/down: OK', flush=True)
            finally:
                run(['pg_ctl', '-D', str(data), '-m', 'immediate', '-w', 'stop'], quiet=True)
        print('verify-phase4: OK', flush=True)
    finally:
        if portable is not None:
            portable.cleanup()


if __name__ == '__main__':
    main()
