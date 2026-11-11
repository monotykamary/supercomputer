"""Download referenced results without expanding bulk JSON into client memory.

``read(ref, offset, count)`` returns raw bytes (including b'' at EOF). All
requests and local copies are bounded by CHUNK_BYTES. References are preserved;
only absolute local filename strings are added to a shallow copy of the row.
"""
import contextlib
import hashlib
import os
from pathlib import Path
import stat
import uuid

from . import resultfiles, resultjson

CHUNK_BYTES = resultfiles.CHUNK_BYTES


@contextlib.contextmanager
def _directory(path):
    """Create private directories and pin every component without symlinks."""
    path = Path(path).absolute()
    fd = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            if part == '..':
                raise ValueError('parent traversal in download directory')
            try:
                os.mkdir(part, mode=0o700, dir_fd=fd)
            except FileExistsError:
                pass
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=fd)
            os.close(fd)
            fd = child
        yield fd
    finally:
        os.close(fd)


def _relative_parts(path):
    if not isinstance(path, str) or not path or '\\' in path or '\x00' in path:
        raise ValueError('invalid output file path')
    parts = path.split('/')
    if any(part in ('', '.', '..') for part in parts):
        raise ValueError('output file path must be relative without traversal')
    return parts


def _name(row):
    name = row.get('name')
    if (not isinstance(name, str) or not name or name in ('.', '..')
            or any(c in name for c in ('/', '\\', '\x00'))):
        raise ValueError('invalid result row name')
    return name


def _copy_file(store, ref, directory, parts):
    """Copy verified content into a same-directory temporary, then publish."""
    with _directory(directory.joinpath(*parts[:-1])) as parent:
        target = parts[-1]
        try:
            info = os.stat(target, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISREG(info.st_mode):
                raise ValueError('output destination is not a regular file')
        temporary = '.download-' + uuid.uuid4().hex
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=parent)
        try:
            with os.fdopen(fd, 'wb') as output:
                digest = hashlib.sha256()
                offset = 0
                while offset < ref['bytes']:
                    chunk = store.read(ref, offset, min(CHUNK_BYTES, ref['bytes'] - offset))
                    if not chunk:
                        raise ValueError('verified output file was truncated')
                    output.write(chunk)
                    digest.update(chunk)
                    offset += len(chunk)
                if offset != ref['bytes'] or digest.hexdigest() != ref['sha256']:
                    raise ValueError('output copy size or digest mismatch')
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, target, src_dir_fd=parent, dst_dir_fd=parent)
            os.fsync(parent)
        finally:
            try:
                os.unlink(temporary, dir_fd=parent)
            except FileNotFoundError:
                pass


def download(row: dict, read: callable, directory: Path, *, verify_cached: bool = False) -> dict:
    """Import verified references and materialize an optional output manifest.

    Large result/row JSON remains on disk, never decoded here. The manifest is
    decoded one bounded descriptor at a time. Each output file is independently
    atomic; a later failure may leave earlier, fully verified files in place.
    No directories are created when the row has no references.
    """
    out = dict(row)
    if not any(key in row for key in ('result_ref', 'row_ref', 'output_files_ref')):
        return out
    directory = Path(directory).absolute()
    store = resultfiles.Store(directory / '.payloads')
    with _directory(store.root):
        pass

    def import_ref(ref):
        ref = dict(resultfiles.validate_ref(ref))
        store.import_ref(ref, lambda offset, count: read(ref, offset, count))
        if verify_cached:
            digest, size = hashlib.sha256(), 0
            with store.open(ref) as source:
                while chunk := source.read(CHUNK_BYTES):
                    digest.update(chunk)
                    size += len(chunk)
            if size != ref['bytes'] or digest.hexdigest() != ref['sha256']:
                raise ValueError('cached result payload size or SHA-256 mismatch')
        return ref, store.root / (ref['sha256'] + '.payload')

    for key, target in (('result_ref', 'result_file'), ('row_ref', 'row_file')):
        if key in row:
            _, path = import_ref(row[key])
            out[target] = str(path)

    if 'output_files_ref' in row:
        files = directory / (_name(row) + '.files')
        manifest_ref = resultfiles.validate_ref(row['output_files_ref'])
        if manifest_ref['format'] != 'json':
            raise ValueError('output manifest must be JSON')
        _, manifest = import_ref(manifest_ref)
        with _directory(files):
            pass
        with _directory(store.root) as parent:
            fd = os.open(manifest.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                         dir_fd=parent)
            with os.fdopen(fd, 'rb') as source:
                if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                    raise ValueError('manifest is not a regular file')
                for descriptor in resultjson.iter_array(source):
                    if not isinstance(descriptor, dict) or 'path' not in descriptor:
                        raise ValueError('invalid output file descriptor')
                    parts = _relative_parts(descriptor['path'])
                    ref = {key: value for key, value in descriptor.items() if key != 'path'}
                    resultfiles.validate_ref(ref)
                    if ref['format'] != 'file':
                        raise ValueError('output descriptor must reference a file')
                    ref, _ = import_ref(ref)
                    _copy_file(store, ref, files, parts)
        out['output_files_manifest'] = str(manifest)
        out['output_files_dir'] = str(files)
    return out
