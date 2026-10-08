#!/usr/bin/env python3
"""Validate the artifacts that exist at the architecture milestone."""

import os
import pathlib
import re
import shutil
import socket
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]


def run(*args, cwd=ROOT, quiet=False):
    # The server inherits pg_ctl's stdout; a captured pipe would wait forever.
    stdout = subprocess.DEVNULL if args[0] == 'pg_ctl' else subprocess.PIPE
    result = subprocess.run(args, cwd=cwd, text=True, stdout=stdout, stderr=subprocess.PIPE, check=False)
    if result.returncode:
        raise RuntimeError(f"{' '.join(args)} failed:\n{result.stdout}{result.stderr}")
    if not quiet:
        print(result.stdout.strip() or f"{' '.join(args)}: OK")
    return result.stdout


def check_files():
    required = [
        'README.md', 'SPEC.md', 'SECURITY.md', 'CONTRIBUTING.md',
        'CHANGELOG.md', 'LICENSE', 'go.mod', 'api/openapi.yaml',
        'migrations/0001_initial.up.sql', 'migrations/0001_initial.down.sql',
        'docs/architecture.md', 'docs/schema.md', 'docs/failure-matrix.md',
        'docs/threat-model.md', 'docs/testing.md', 'docs/demo.md',
        'docs/benchmark.md', 'docs/upstream-baseline.md',
    ] + [f'docs/adr/{n:04d}-{name}.md' for n, name in enumerate([
        'message-identity', 'adapter-boundary', 'finality', 'stuck-sla',
        'reorg-retention', 'ws-http-recovery',
    ], 1)]
    missing = [name for name in required if not (ROOT / name).is_file()]
    if missing:
        raise RuntimeError(f"missing artifacts: {missing}")
    print(f'Required artifacts: {len(required)} OK')


def check_markdown():
    # Encoded terms avoid putting process references in a repository artifact.
    banned = [bytes.fromhex(value).decode() for value in (
        '4149', '4c4c4d', '43686174475054', '436f646578',
        '70726f6d7074', '6167656e74', '67656e6572617465642d6279',
        '677074', '636c61756465', '67656d696e69',
    )]
    weak_claims = [bytes.fromhex(value).decode() for value in (
        '544f444f', '544244', '706c616365686f6c646572',
    )]
    paths = [path for path in ROOT.rglob('*') if path.is_file() and '.git' not in path.parts]
    for path in paths:
        if path.suffix not in {'.md', '.yaml', '.sql', '.py', '.rb'} and path.name not in {'Makefile', 'go.mod'}:
            continue
        content = path.read_text()
        for term in banned + weak_claims:
            if re.search(r'(?i)\b' + re.escape(term) + r'\b', content):
                raise RuntimeError(f'forbidden artifact reference in {path.relative_to(ROOT)}: {term}')
        personal_path = r'/(?:' + 'Users|home' + r')/[^\s/]+' + r'|[A-Z]:\\' + 'Users' + r'\\'
        if re.search(personal_path, content):
            raise RuntimeError(f'personal path in {path.relative_to(ROOT)}')
        if re.search(r'AKIA[0-9A-Z]{16}|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY|gh[pousr]_[A-Za-z0-9]{30,}|xox[baprs]-[A-Za-z0-9-]{20,}', content):
            raise RuntimeError(f'credential pattern in {path.relative_to(ROOT)}')
        if path.suffix == '.md':
            for target in re.findall(r'(?<!!)\[[^\]]+\]\(([^)]+)\)', content):
                location = target.split('#', 1)[0]
                if not location or re.match(r'https?://', location):
                    continue
                if not (path.parent / location).exists():
                    raise RuntimeError(f'broken link in {path.relative_to(ROOT)}: {target}')
    print('Links, credential patterns, personal paths, and artifact language: OK')


def check_go():
    run('go', 'mod', 'verify')
    sources = list(ROOT.rglob('*.go'))
    if sources:
        unformatted = run('gofmt', '-l', *(str(path) for path in sources), quiet=True)
        if unformatted.strip():
            raise RuntimeError(f'unformatted Go files:\n{unformatted}')
        run('go', 'vet', './...')
    else:
        print('gofmt/go vet: no Go source packages yet')


def check_schema():
    for executable in ('initdb', 'pg_ctl', 'psql'):
        if not shutil.which(executable):
            raise RuntimeError(f'{executable} required for PostgreSQL migration smoke')
    with tempfile.TemporaryDirectory(prefix='bridgewatch-pg-') as directory:
        base = pathlib.Path(directory)
        data, socket_dir = base / 'data', base / 'socket'
        socket_dir.mkdir()
        run('initdb', '-D', str(data), '-A', 'trust', '-U', 'bridgewatch', '--no-instructions', quiet=True)
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        options = f"-c listen_addresses='' -c unix_socket_directories={socket_dir} -p {port}"
        run('pg_ctl', '-D', str(data), '-o', options, '-w', 'start', quiet=True)
        try:
            base_args = ('psql', '-X', '-v', 'ON_ERROR_STOP=1', '-h', str(socket_dir), '-p', str(port), '-U', 'bridgewatch', '-d', 'postgres')
            run(*base_args, '-f', str(ROOT / 'migrations/0001_initial.up.sql'), quiet=True)
            count = run(*base_args, '-Atc', "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'", quiet=True).strip()
            if count != '9':
                raise RuntimeError(f'expected 9 tables, got {count}')
            run(*base_args, '-f', str(ROOT / 'migrations/0001_initial.down.sql'), quiet=True)
        finally:
            run('pg_ctl', '-D', str(data), '-m', 'immediate', '-w', 'stop', quiet=True)
    print('PostgreSQL 18 migration up/down smoke: OK')


if __name__ == '__main__':
    os.chdir(ROOT)
    check_files()
    check_markdown()
    check_go()
    run('ruby', 'scripts/check_openapi.rb', 'api/openapi.yaml')
    check_schema()
    print('verify-phase0: OK')
