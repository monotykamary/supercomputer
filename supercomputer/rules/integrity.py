"""The integrity rules of the store and of the volumes' copy: what is kept, placed, consumed or recorded only when the
bytes are what their hashes and manifests say. Standard library only (typing): this file is also the head of the
scripts that run on the VMs (volumes.py's receiver and check, store.py's range server and check) and is shipped into
the Modal image of modal_volumes.py, so each of those runs these very classes, not a copy of them.

    Manifest       a directory against its manifest: every entry as recorded, nothing more, nothing less
    Ranges         the range server's spans: never past the file, never negative
    PartOrder      the receiver's parts: kept only when their sha256 is the one sent, consumed only in index order
    ReceiverGate   the receiver's door: the token in the path and in the header, names it can take
    PullFile       a pull: a file placed only from bytes whose whole sha256 is its digest, never outside its directory
    RangeFetch     a ranged fetch: resumed where it stopped, given up only after `limit` failures in a row
    Tries          a call tried again a few times, then given up (the copy's _retry and _client)
    CopyGates      a volume's copy: recorded only after every check passed, its snapshot's tree included
    Checks         the decisions of the checks themselves (a restore, a decryption, a store push, a warm recheck)
    Sizes          the limits a copy or a push stays within
    Secrets        the split of a volume's secret-looking files from the rest
    Serving        which serving VMs and copy tickets go
"""
from typing import Literal

Arrival = Literal["dup", "bad", "kept"]


class Manifest:
    """A directory against its manifest ({path: [size, sha256]}): `differ` the entries whose size or sha256 differ, or
    that one side has and the other doesn't (the symmetric difference), `here` and `want` the two sides' counts."""

    # gdp: law verify passes only when every manifest entry matches: not result or (differ == 0 and here == want)
    def verify(self, differ: int, here: int, want: int) -> bool:
        return differ == 0 and here == want


class PartOrder:
    """volumes.py's receiver: a stream's parts PUT in any order by several workers, each with its sha256; a part is kept
    only when its bytes hash to the sha256 sent, and the stream is consumed (written to tar) strictly in index order up
    to the count the sender ends it with."""

    # gdp: law an ended stream is complete only once every part was consumed: not ended or not complete or next >= end

    def __init__(self):
        self.next = 0  # the next part to consume
        self.ended = False
        self.end = 0  # the parts the sender says there are
        self.complete = False

    # gdp: law a part whose sha256 differs is never kept: result != "kept" or sha_ok
    # gdp: law a part already consumed is a duplicate: i >= next or result == "dup"
    def arrive(self, i: int, sha_ok: bool) -> Arrival:
        if i < self.next:
            return "dup"
        if not sha_ok:
            return "bad"
        return "kept"

    # gdp: law a part is consumed only in index order: next == old(next) or (i == old(next) and next == old(next) + 1)
    # gdp: law nothing is consumed past the stream's end: not result or not old(ended) or old(next) < end
    def consume(self, i: int) -> bool:
        if i != self.next or (self.ended and self.next >= self.end):
            return False
        self.next += 1
        if self.ended and self.next >= self.end:
            self.complete = True
        return True

    def finish(self, parts: int) -> None:
        if self.ended:
            return
        self.ended = True
        self.end = parts
        self.complete = self.next >= parts


class ReceiverGate:
    """The receiver's door: a PUT is taken only with the token in its path and in its header, for a name it can take
    (letters, digits, . _ -, not starting with one of the last three); an end is ok only when the stream was consumed
    whole with no error."""

    # gdp: law no PUT is taken without the token in its path and its header: not result or (token_path and token_header and name_ok)
    def admit(self, token_path: bool, token_header: bool, name_ok: bool) -> bool:
        return token_path and token_header and name_ok

    # gdp: law an end is ok only when the stream was consumed whole with no error: not result or (complete and not error)
    def end_ok(self, complete: bool, error: bool) -> bool:
        return complete and not error


class Ranges:
    """store.py's range server: a request for bytes a-b (b missing: to the end) serves a..min(b, size - 1); a range
    starting past the end serves nothing."""

    # gdp: law a range never ends past the file: result <= size - 1
    def last(self, b: int, has_b: bool, size: int) -> int:
        return min(b, size - 1) if has_b else size - 1

    # gdp: law a range served has no negative length: result >= 0
    def length(self, a: int, last: int) -> int:
        return max(0, last - a + 1)


class PullFile:
    """store.pull: a file of the manifest placed from the cache by its digest; a digest fetched in ranges goes into the
    cache only when the whole file's sha256 is the digest; nothing is placed outside the pull's directory (a manifest
    path or a link's path with `..` or a leading /)."""

    # gdp: law a fetched file is cached only when its whole sha256 matches its digest: not result or got == digest
    def cache(self, digest: str, got: str) -> bool:
        return got == digest

    # gdp: law nothing is placed outside the pull's directory: not result or inside
    def place(self, inside: bool) -> bool:
        return inside


class RangeFetch:
    """get_range: bytes a..b of a file, resumed where a response stopped short; a failure in a row past `limit` gives up,
    progress resets the count."""

    # gdp: law a fetch gives up only after more than limit failures in a row: state != "failed" or fails > limit
    # gdp: law a fetch done has every byte: state != "done" or left == 0
    # gdp: law a fetch still going has bytes left and failures to spare: state != "going" or (left > 0 and fails <= limit)
    # gdp: law a fetch ends: state == "going" leads to state != "going" by left, limit + 1 - fails via fail()

    def __init__(self, size: int, limit: int):
        if size < 1 or limit < 0:
            raise ValueError("a fetch of at least one byte, a limit of at least 0")
        self.limit = limit
        self.left = size
        self.fails = 0
        self.state: Literal["going", "done", "failed"] = "going"

    def got(self, n: int) -> None:
        """n bytes more arrived (at least one: a response with none is a failure)."""
        if self.state != "going" or n < 1:
            return
        self.left -= min(n, self.left)
        self.fails = 0
        if self.left == 0:
            self.state = "done"

    def fail(self) -> None:
        if self.state != "going":
            return
        self.fails += 1
        if self.fails > self.limit:
            self.state = "failed"


class Tries:
    """A call tried again: at most `tries` attempts, then given up (modal_volumes's _retry, 10; its _client, 6)."""

    # gdp: law a call is tried at least once: tries >= 1
    # gdp: law at most tries attempts: used <= tries
    # gdp: law a call still trying has an attempt left: state != "trying" or used < tries
    # gdp: law a call ends: state == "trying" leads to state != "trying" by tries - used via attempt(ok = false)

    def __init__(self, tries: int):
        if tries < 1:
            raise ValueError("tries is at least 1")
        self.tries = tries
        self.used = 0
        self.state: Literal["trying", "done", "failed"] = "trying"

    def attempt(self, ok: bool) -> None:
        if self.state != "trying":
            return
        self.used += 1
        if ok:
            self.state = "done"
        elif self.used >= self.tries:
            self.state = "failed"


Stage = Literal["start", "restored", "room", "received", "checked", "decrypted", "snapshot", "tree", "recorded", "failed"]


class CopyGates:
    """volumes.copy_one: a volume's copy goes through its gates in order: the previous store's restore (when the VM
    starts from one), the disk's room, the receive, the copy's check against Modal's manifest (its manifest's sha256
    included: the one Modal sent, never one missing), the decryptions, the snapshot ready, the snapshot's tree; it is
    recorded only after all of them, and a failed gate ends it."""

    # gdp: law a copy is recorded only after every gate passed: state != "recorded" or (restored and room and received and checked and decrypted and snapshot and tree)
    # gdp: law a copy past its restore had it: state == "start" or state == "failed" or restored
    # gdp: law a copy past its disk had room: state == "start" or state == "restored" or state == "failed" or room
    # gdp: law a copy past its receive had it: state == "start" or state == "restored" or state == "room" or state == "failed" or received
    # gdp: law a copy past its check passed it: not (state == "checked" or state == "decrypted" or state == "snapshot" or state == "tree" or state == "recorded") or checked
    # gdp: law a copy past its decryption passed it: not (state == "decrypted" or state == "snapshot" or state == "tree" or state == "recorded") or decrypted
    # gdp: law a copy past its snapshot had it ready: not (state == "snapshot" or state == "tree" or state == "recorded") or snapshot
    # gdp: law a copy past its tree passed it: not (state == "tree" or state == "recorded") or tree

    def __init__(self):
        self.state: Stage = "start"
        self.restored = False
        self.room = False
        self.received = False
        self.checked = False
        self.decrypted = False
        self.snapshot = False
        self.tree = False

    def _gate(self, ok: bool) -> bool:
        if not ok:
            self.state = "failed"
        return ok

    def restore(self, after: bool, ok: bool) -> bool:
        """The previous store re-hashed on the VM from its snapshot (`after`: the VM starts from one; else nothing to check)."""
        if self.state != "start" or not self._gate(not after or ok):
            return False
        self.restored = True
        self.state = "restored"
        return True

    def disk(self, free: int, need: int) -> bool:
        if self.state != "restored" or not self._gate(free >= need):
            return False
        self.room = True
        self.state = "room"
        return True

    def receive(self, ok: bool) -> bool:
        if self.state != "room" or not self._gate(ok):
            return False
        self.received = True
        self.state = "received"
        return True

    # gdp: law a copy's check passes only when every file and link matches and its manifest's sha256 is the one sent: not result or (nbad == 0 and nbadlinks == 0 and files == want and want == sent and sha_given and sha_ok)
    def check(self, nbad: int, nbadlinks: int, files: int, want: int, sent: int, sha_given: bool, sha_ok: bool) -> bool:
        if self.state != "received" or not self._gate(nbad == 0 and nbadlinks == 0 and files == want and want == sent and sha_given and sha_ok):
            return False
        self.checked = True
        self.state = "checked"
        return True

    def decrypt(self, needed: bool, ok: bool) -> bool:
        if self.state != "checked" or not self._gate(not needed or ok):
            return False
        self.decrypted = True
        self.state = "decrypted"
        return True

    def saved(self, ready: bool) -> bool:
        if self.state != "decrypted" or not self._gate(ready):
            return False
        self.snapshot = True
        self.state = "snapshot"
        return True

    # gdp: law a snapshot's tree passes only when every size and link it lists is as recorded: not result or (nbad == 0 and extra == 0 and nbadlinks == 0)
    def tree_ok(self, nbad: int, extra: int, nbadlinks: int) -> bool:
        """The snapshot's file tree (boat's listing; a listing boat cut short is judged on what it lists)."""
        if self.state != "snapshot" or not self._gate(nbad == 0 and extra == 0 and nbadlinks == 0):
            return False
        self.tree = True
        self.state = "tree"
        return True

    def record(self) -> bool:
        if self.state != "tree":
            return False
        self.state = "recorded"
        return True


class Checks:
    """The checks' own decisions."""

    # gdp: law a restore passes only when every file and link matches and the manifest's sha256 is the one recorded: not result or (nbad == 0 and nbadlinks == 0 and files == want and recorded and sha_ok)
    def restore(self, nbad: int, nbadlinks: int, files: int, want: int, recorded: bool, sha_ok: bool) -> bool:
        """volumes.restore_check: a store re-hashed on a VM from its snapshot (`recorded`: the store's entry has the
        manifest's sha256 the copy saw; with none, nothing to compare, so not passed)."""
        return nbad == 0 and nbadlinks == 0 and files == want and recorded and sha_ok

    # gdp: law a decryption passes only when every hash, the magic and the MAC match: not result or (cipher_ok and magic_ok and mac_ok and plain_ok and nbad == 0 and manifest_ok)
    def decrypted(self, cipher_ok: bool, magic_ok: bool, mac_ok: bool, plain_ok: bool, nbad: int, manifest_ok: bool) -> bool:
        """volumes.decrypt_check: the ciphertext's sha256, its magic (SCV1), the HMAC, the plaintext's sha256, every
        file against the plaintext manifest, the manifest's sha256."""
        return cipher_ok and magic_ok and mac_ok and plain_ok and nbad == 0 and manifest_ok

    # gdp: law a push is kept only when the VM's copy matches the manifest whole: not result or (nbad == 0 and files == entries)
    def pushed(self, nbad: int, files: int, entries: int) -> bool:
        """store.push: the VM's check against the manifest sent."""
        return nbad == 0 and files == entries

    # gdp: law a check runs again only on a warm VM where files differed: not result or (warm and nbad > 0)
    def again(self, warm: bool, nbad: int) -> bool:
        """volumes.check_dir: on a VM from a snapshot (its disk filled lazily), once more where files differed."""
        return warm and nbad > 0


class Sizes:
    """The limits: a push or a fetch from Modal within max_bytes, a copy only onto a disk with room for it, the
    encryption in memory within 1 GiB, the kept-apart secrets within 512 MiB."""

    # gdp: law nothing past its limit goes: not result or size <= limit
    def fits(self, size: int, limit: int) -> bool:
        return size <= limit


class Secrets:
    """modal_volumes's split: a file's content is read for credentials only when it is text (no NUL in its first 4 KB)
    of at most TEXT_MAX bytes, within the scan's time allowance; a file with any secret kind goes to the encrypted stash,
    never into the plain manifest."""

    # gdp: law content is scanned only in small text files within the time allowed: not result or (size <= text_max and not binary and not partial)
    def scan_content(self, size: int, text_max: int, binary: bool, partial: bool) -> bool:
        return size <= text_max and not binary and not partial

    # gdp: law a secret-looking file never enters the plain manifest: result != "plain" or not secret
    def route(self, secret: bool) -> Literal["stash", "plain"]:
        return "stash" if secret else "plain"

    # gdp: law an empty stream still goes as one part: emitted != 0 or result
    def last_part(self, buffered: int, emitted: int) -> bool:
        """_Parts.close: the bytes still buffered go as a last part, and an empty stream as one empty part."""
        return buffered > 0 or emitted == 0


class Serving:
    """store.unserve and volumes.cleanup: which VMs go."""

    # gdp: law a serving VM goes only when asked for all, for its name, or past its minutes: not result or all_ or named or expired
    def unserve(self, all_: bool, named: bool, expired: bool) -> bool:
        return all_ or named or expired

    # gdp: law a copy's VM goes only when its client is dead: not result or not client_alive
    # gdp: law a copy's VM goes once its client is dead: client_alive or result
    def cleanup(self, client_alive: bool) -> bool:
        """volumes.cleanup: a ticketed VM of a copy is deleted only when the client that holds it is dead (its pid, on
        the host it was written on)."""
        return not client_alive

    # gdp: law a stored name is copied again only when asked or not ready: not result or redo or not done
    def copy_again(self, done: bool, redo: bool) -> bool:
        """volumes main: a name already stored (its record, its snapshot ready) is skipped unless --redo."""
        return redo or not done
