#!/usr/bin/env python3
"""Run source-chain checks with isolated PostgreSQL and pinned Foundry tools."""

import os
import pathlib
import platform
import hashlib
import re
import shutil
import socket
import subprocess
import tarfile
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]


def phase2_guards():
    source_runtime = [path for path in (ROOT / 'internal' / 'source').glob('*.go') if not path.name.endswith('_test.go')]
    if any('COMPLETED' in path.read_text() for path in source_runtime):
        raise RuntimeError('completion state in source runtime')

    blocked = [bytes.fromhex(value).decode() for value in (
        '4149', '4c4c4d', '43686174475054', '436f646578',
        '70726f6d7074', '6167656e74', '67656e6572617465642d6279',
        '677074', '636c61756465', '67656d696e69',
    )]
    extensions = {'.go', '.md', '.py', '.rb', '.sol', '.sql', '.toml', '.yaml', '.yml'}
    for path in ROOT.rglob('*'):
        if not path.is_file() or '.git' in path.parts:
            continue
        if path.suffix not in extensions and path.name not in {'Makefile', 'go.mod'}:
            continue
        content = path.read_text()
        for term in blocked:
            if re.search(r'(?i)\b' + re.escape(term) + r'\b', content):
                raise RuntimeError(f'forbidden artifact reference in {path.relative_to(ROOT)}: {term}')
        personal_path = r'/(?:' + 'Users|home' + r')/[^\s/]+' + r'|[A-Z]:\\' + 'Users' + r'\\'
        if re.search(personal_path, content):
            raise RuntimeError(f'personal path in {path.relative_to(ROOT)}')
        if re.search(r'AKIA[0-9A-Z]{16}|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY|gh[pousr]_[A-Za-z0-9]{30,}|xox[baprs]-[A-Za-z0-9-]{20,}', content):
            raise RuntimeError(f'credential pattern in {path.relative_to(ROOT)}')
    print('Phase 2 boundaries and repository hygiene: OK', flush=True)


def run(args, env=None, quiet=False):
    stdout = subprocess.DEVNULL if args[0] == 'pg_ctl' else subprocess.PIPE
    result = subprocess.run(args, cwd=ROOT, env=env, text=True, stdout=stdout, stderr=subprocess.PIPE)
    if result.returncode:
        raise RuntimeError(f"{' '.join(args)} failed:\n{result.stdout or ''}{result.stderr}")
    if not quiet:
        print((result.stdout or '').strip() or f'{args[0]}: OK', flush=True)
    return result.stdout or ''


def foundry_tools():
    supplied = os.environ.get('BRIDGEWATCH_FOUNDRY_BIN')
    if supplied:
        base = pathlib.Path(supplied)
        return str(base/'forge'), str(base/'anvil'), None
    forge, anvil = shutil.which('forge'), shutil.which('anvil')
    if forge and anvil and '1.8.5' in run([forge,'--version'],quiet=True) and '1.8.5' in run([anvil,'--version'],quiet=True):
        return forge, anvil, None
    machine = platform.machine().lower()
    system = platform.system().lower()
    if system == 'darwin' and machine in ('arm64','aarch64'):
        suffix = 'darwin_arm64'
    elif system == 'linux' and machine in ('x86_64','amd64'):
        suffix = 'linux_amd64'
    elif system == 'linux' and machine in ('arm64','aarch64'):
        suffix = 'linux_arm64'
    else:
        raise RuntimeError('set BRIDGEWATCH_FOUNDRY_BIN for this platform')
    directory = tempfile.TemporaryDirectory(prefix='bridgewatch-foundry-')
    asset = f'foundry_v1.8.5_{suffix}.tar.gz'
    base_url = 'https://github.com/foundry-rs/foundry/releases/download/v1.8.5/'
    archive = pathlib.Path(directory.name)/asset
    checksum = pathlib.Path(directory.name)/(asset+'.sha256')
    retry = ['--retry', '5', '--retry-all-errors', '--retry-delay', '1']
    run(['curl','-fsSL',*retry,base_url+asset,'-o',str(archive)],quiet=True)
    run(['curl','-fsSL',*retry,base_url+asset.replace('.tar.gz','.sha256'),'-o',str(checksum)],quiet=True)
    expected = checksum.read_text().split()[0]
    digest = hashlib.sha256()
    with archive.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024*1024),b''):
            digest.update(chunk)
    if digest.hexdigest()!=expected:
        directory.cleanup()
        raise RuntimeError('Foundry archive checksum mismatch')
    with tarfile.open(archive) as bundle:
        for member in bundle.getmembers():
            if pathlib.PurePosixPath(member.name).name != member.name:
                raise RuntimeError('unexpected Foundry archive path')
        bundle.extractall(directory.name)
    base = pathlib.Path(directory.name)
    return str(base/'forge'), str(base/'anvil'), directory


def main():
    run(['make', 'verify-phase1'])
    phase2_guards()
    for command in ('go','initdb','pg_ctl','psql'):
        if not shutil.which(command): raise RuntimeError(f'missing command: {command}')
    forge, anvil, portable = foundry_tools()
    forge_version = run([forge, '--version'], quiet=True)
    anvil_version = run([anvil, '--version'], quiet=True)
    if '1.8.5' not in forge_version or '1.8.5' not in anvil_version:
        raise RuntimeError('Foundry 1.8.5 required for Phase 2 evidence')
    print(run(['go','version'],quiet=True).strip(), flush=True)
    print(run(['postgres','--version'],quiet=True).strip(), flush=True)
    print(forge_version.splitlines()[0], anvil_version.splitlines()[0], flush=True)
    run([forge,'build'])
    go_files = [str(path) for path in (ROOT/'internal').rglob('*.go')]
    if run(['gofmt','-l',*go_files],quiet=True).strip(): raise RuntimeError('unformatted Go source')
    run(['go','mod','verify'])
    run(['go','vet','./...'])
    with tempfile.TemporaryDirectory(prefix='bridgewatch-phase2-') as directory:
        base=pathlib.Path(directory);data, socket_dir=base/'data',base/'socket';socket_dir.mkdir()
        run(['initdb','-D',str(data),'-A','trust','-U','bridgewatch','--no-instructions'],quiet=True)
        with socket.socket() as probe:
            probe.bind(('127.0.0.1',0));port=probe.getsockname()[1]
        options=f"-c listen_addresses='' -c unix_socket_directories={socket_dir} -p {port}"
        run(['pg_ctl','-D',str(data),'-o',options,'-w','start'],quiet=True)
        try:
            psql=['psql','-X','-v','ON_ERROR_STOP=1','-h',str(socket_dir),'-p',str(port),'-U','bridgewatch','-d','postgres']
            for name in ('0001_initial.up.sql','0002_phase1_core.up.sql','0003_source_watcher.up.sql','0004_destination_watcher.up.sql','0005_operations.up.sql'):
                run([*psql,'-f',str(ROOT/'migrations'/name)],quiet=True)
            source_schema = run([*psql,'-Atc', "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('source_streams','source_reorgs')"], quiet=True).strip()
            if source_schema != '2':
                raise RuntimeError(f'expected source schema tables, found {source_schema}')
            env=os.environ.copy();url=f'host={socket_dir} port={port} user=bridgewatch dbname=postgres sslmode=disable'
            env['BRIDGEWATCH_TEST_SOURCE_DB_URL']=url
            env['BRIDGEWATCH_TEST_DATABASE_URL']=url
            env['BRIDGEWATCH_ANVIL_BIN']=anvil
            run(['go','test','-p','1','./...'],env=env)
            run(['go','test','-race','-p','1','./...'],env=env)
            run(['go','test','-count=1','-v','-run','^TestRealAnvilSourceCatchup$','./internal/source'],env=env)
            run(['go','test','./internal/source','-run','^$','-fuzz','^FuzzBoundRange$','-fuzztime=100x','-parallel=1'],env=env)
            run(['go','test','./internal/source','-run','^$','-fuzz','^FuzzCommonAncestor$','-fuzztime=100x','-parallel=1'],env=env)
            for name in ('0005_operations.down.sql','0004_destination_watcher.down.sql','0003_source_watcher.down.sql','0002_phase1_core.down.sql','0001_initial.down.sql'):
                run([*psql,'-f',str(ROOT/'migrations'/name)],quiet=True)
            print('PostgreSQL 18 migrations up/down: OK',flush=True)
        finally:
            run(['pg_ctl','-D',str(data),'-m','immediate','-w','stop'],quiet=True)
    if portable is not None: portable.cleanup()
    print('verify-phase2: OK',flush=True)

if __name__=='__main__': main()
