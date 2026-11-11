"""Bounded Boat exec transport for legacy agent result responses.

The caller supplies a streaming decoder; neither exec stdout nor this module
materializes the response. This transport never retries the HTTP request.
"""
import base64
import json
import secrets
import sys

CHUNK_BYTES = 64 * 1024
_META_BYTES = 1024

# Static program and positional arguments only, including the token and paths.
_REMOTE = r'''
import base64, json, os, stat, sys, tempfile, urllib.request, urllib.error

def opened(path, flags):
    fd = os.open(path, flags | os.O_NOFOLLOW)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError('not regular')
        return fd
    except BaseException:
        os.close(fd)
        raise

def run():
    op, path = sys.argv[1:3]
    if op == 'clean':
        for p in sys.argv[2:]:
            try:
                os.unlink(p)
            except FileNotFoundError:
                pass
        return
    if op == 'stage':
        offset = int(sys.argv[3])
        flags = os.O_WRONLY | (os.O_CREAT | os.O_EXCL if offset == 0 else 0)
        fd = os.open(path, flags | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'r+b') as f:
            if not stat.S_ISREG(os.fstat(f.fileno()).st_mode) or os.fstat(f.fileno()).st_size != offset:
                raise ValueError('stage offset')
            f.seek(offset)
            f.write(sys.argv[4].encode('ascii'))
        return
    if op == 'read':
        size, offset, count = map(int, sys.argv[3:6])
        if not 0 <= offset <= size or not 0 <= count <= 65536:
            raise ValueError('read bounds')
        with os.fdopen(opened(path, os.O_RDONLY), 'rb') as f:
            if os.fstat(f.fileno()).st_size != size:
                raise ValueError('changed size')
            f.seek(offset)
            data = f.read(count)
            if len(data) != min(count, size - offset):
                raise ValueError('short read')
            if offset + len(data) == size and f.read(1):
                raise ValueError('expected EOF')
        print(base64.b64encode(data).decode('ascii'))
        return
    if op != 'post':
        raise ValueError('operation')
    arg, token, port, timeout = sys.argv[3:7]
    if arg.startswith('@'):
        with os.fdopen(opened(arg[1:], os.O_RDONLY), 'rb') as f:
            encoded = f.read(5333337)
            if f.read(1):
                raise ValueError('request too big')
    else:
        encoded = arg.encode('ascii')
    request = urllib.request.Request('http://127.0.0.1:' + port + '/rpc',
        data=base64.b64decode(encoded, validate=True),
        headers={'X-SC-Token': token, 'Content-Type': 'application/json'})
    try:
        response = urllib.request.urlopen(request, timeout=float(timeout))
    except urllib.error.HTTPError as error:
        response = error
    with response:
        # NamedTemporaryFile creates mode 0600 exclusively. Publish the inode
        # under the caller's unguessable exact name without overwriting anything.
        with tempfile.NamedTemporaryFile(prefix='sc-result-', dir='/tmp') as f:
            os.fchmod(f.fileno(), 0o600)
            os.link(f.name, path, follow_symlinks=False)
            size = 0
            while True:
                chunk = response.read(256 * 1024)
                if not chunk:
                    break
                f.write(chunk)
                size += len(chunk)
            f.flush()
        print(json.dumps({'path': path, 'bytes': size, 'status': response.code}))

try:
    run()
except Exception as error:
    # HTTP bodies and arbitrary exception strings must never escape on stdout.
    sys.stderr.write('result relay: ' + type(error).__name__ + '\n')
    sys.exit(1)
'''


class _Stream:
    def __init__(self, call, path, size):
        self.call, self.path, self.size = call, path, size
        self.offset = 0
        self.eof = False

    def read(self, n):
        if type(n) is not int or n < 0:
            raise ValueError('result relay requires a bounded read')
        if not n or self.eof:
            return b''
        count = min(n, CHUNK_BYTES)
        text = self.call('read', self.path, str(self.size), str(self.offset), str(count),
                         limit=4 * ((CHUNK_BYTES + 2) // 3) + 1)
        try:
            data = base64.b64decode(text.rstrip('\n'), validate=True)
        except (ValueError, UnicodeError) as error:
            raise ValueError('invalid result relay chunk') from error
        if len(data) != min(count, self.size - self.offset):
            raise ValueError('incorrect result relay chunk length')
        self.offset += len(data)
        self.eof = self.offset == self.size
        return data


def exec_response(client, body, timeout, decode):
    """Return ``(HTTP status, decode(binary_stream))`` via bounded exec calls.

    A decoder must consume the full response. Remote cleanup also runs when
    metadata validation, transfer, HTTP transport, or decoding fails.
    """
    from . import boat

    if len(body) >= boat.EXEC_BODY:
        raise ValueError('result relay request exceeds exec body limit')
    path = '/tmp/sc-result-' + secrets.token_hex(24)
    request_path = path + '.request'

    def call(op, *args, limit=_META_BYTES):
        result = boat.exec_(client.vm, ['python3', '-c', _REMOTE, op, *args], timeout + 15)
        if result.get('exitCode') != 0:
            raise boat.AgentError('result relay exec failed: ' +
                                  str(result.get('stderr') or result.get('error') or '')[-160:])
        text = result.get('stdout', '')
        if not isinstance(text, str) or len(text) > limit:
            raise boat.AgentError('result relay exec output exceeds bound')
        return text

    try:
        encoded = base64.b64encode(body).decode('ascii')
        arg = encoded
        if len(encoded) > boat.EXEC_ARG:
            for offset in range(0, len(encoded), boat.EXEC_ARG):
                call('stage', request_path, str(offset), encoded[offset:offset + boat.EXEC_ARG])
            arg = '@' + request_path
        meta = json.loads(call('post', path, arg, client.token, str(boat.AGENT_PORT), str(timeout)))
        if (not isinstance(meta, dict) or meta.get('path') != path or
                type(meta.get('bytes')) is not int or meta['bytes'] < 0 or
                type(meta.get('status')) is not int or not 100 <= meta['status'] <= 599):
            raise ValueError('invalid result relay metadata')
        stream = _Stream(call, path, meta['bytes'])
        value = decode(stream)
        if stream.offset != stream.size:
            raise ValueError('result relay decoder did not consume response')
        if not stream.eof:
            stream.read(1)  # Verify even a zero-byte file through O_NOFOLLOW.
        return meta['status'], value
    finally:
        failed = sys.exc_info()[0] is not None
        try:
            call('clean', path, request_path)
        except Exception:
            if not failed:
                raise
