"""Manifest-first service bootstrap; executable without importing supercomputer.

The systemd unit embeds this stdlib-only source and starts it with Python -I.
Never import scheduler code to decide which scheduler code is installed.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys


def source_hash(package: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(package.rglob('*.py')):
        digest.update(str(path.relative_to(package)).encode() + b'\0' + path.read_bytes() + b'\0')
    return digest.hexdigest()[:12]


def installed(manifest: Path) -> tuple[bytes, Path, str]:
    """Resolve only a complete, hash-matched immutable install, never a checkout."""
    raw = manifest.read_bytes()
    record = json.loads(raw)
    if not isinstance(record, dict) or not re.fullmatch(r'[0-9a-f]{12}', str(record.get('code', ''))):
        raise ValueError('install.json has no valid code hash')
    code = record['code']
    source = manifest.parent.resolve() / 'code' / code
    for field in ('source', 'snapshot'):
        value = record.get(field)
        if not isinstance(value, str) or not Path(value).is_absolute() or Path(value).resolve() != source:
            raise ValueError(f'install.json {field} does not name immutable code/{code}')
    package = source / 'supercomputer'
    if source.resolve() != source or not package.is_dir() or package.is_symlink():
        raise ValueError(f'installed snapshot {code} is missing or symlinked')
    for name in ('__init__.py', '__main__.py', 'scheduler.py'):
        if not (package / name).is_file():
            raise ValueError(f'installed snapshot {code} lacks {name}')
    if any(path.is_symlink() for path in package.rglob('*')) or source_hash(package) != code:
        raise ValueError(f'installed snapshot {code} failed source hash verification')
    python = record.get('prepared', {}).get('python')
    if (not isinstance(python, str) or not Path(python).is_absolute()
            or not Path(python).is_file() or not os.access(python, os.X_OK)):
        raise ValueError(f'installed snapshot {code} has no executable prepared interpreter')
    return raw, source, python


def launch(manifest: Path, key_file: Path):
    raw, source, python = installed(manifest)
    env = {**os.environ, 'PYTHONPATH': str(source), 'SUPERCOMPUTER_SRC': str(source)}
    # Preserve the old service's tmpfs-only credential contract. Missing keys
    # are repaired by controller rekey, not read from a persistent login file.
    try:
        env['BOAT_API_KEY'] = key_file.read_text().strip()
    except FileNotFoundError:
        env['BOAT_API_KEY'] = ''
    # A concurrent stage is not permission to boot an arbitrary former target.
    # Fail closed; systemd can retry against the newly published generation.
    if manifest.read_bytes() != raw:
        raise ValueError('install.json changed during service startup; retry the current install')
    os.chdir(source)
    os.execve(python, [python, '-P', '-m', 'supercomputer', 'scheduler', 'serve', '--idle-exit', '0'], env)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('manifest', type=Path)
    parser.add_argument('--key-file', type=Path, default=Path('/run/sc-boat/boat-key'))
    args = parser.parse_args()
    try:
        launch(args.manifest, args.key_file)
    except (OSError, ValueError, TypeError, AttributeError) as exc:
        print(f'scheduler bootstrap refused: {exc}', file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
