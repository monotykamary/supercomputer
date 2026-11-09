"""The integrity and agent rules' broken variants, kept: each the bug a law of supercomputer/rules/integrity.py or of
the agent's rules (supercomputer/agent.py) rules out, as the code had it, with that law stated again here.
tests/check_laws.sh asserts every law below comes back broken, with the run that breaks it."""
from supercomputer.agent import BlobStore, CpuGrant, JobReport, Report
from supercomputer.rules.integrity import Checks, CopyGates, PartOrder, PullFile, Serving


class Oversubscribed(CpuGrant):
    """agent.start before: with fewer CPUs free than asked, CPUs 0..want-1 anyway, held as if they were free."""

    # gdp: law CPUs held never exceed the VM's: held <= cpus
    def grant(self, want: int) -> int:
        k = min(max(1, want), self.cpus)
        self.held += k
        return k


class SpawnFailedRuns(JobReport):
    """agent.report before: an item whose process never started (no pid) is "being started" for ever."""

    # gdp: law an item with no process is running only while it is being started: has_pid or done_file or acked or age_s <= starting_s or result == "lost"
    def read(self, acked: bool, done_file: bool, readable: bool, has_pid: bool, alive: bool, age_s: int) -> Report:
        if acked:
            self.result = "skip"
        elif done_file:
            self.result = "done" if readable else "torn"
        elif has_pid:
            self.result = "running" if alive else "lost"
        else:
            self.result = "running"
        return self.result


class TreeOnTrust(BlobStore):
    """agent._make_tree before: the client's tree id taken on trust (ingest checked it; the warm agent's tree didn't)."""

    # gdp: law no tree is made under an id its manifest doesn't hash to (nor with a blob missing): not result or (have_all and computed == tid)
    def may_make_tree(self, tid: str, computed: str, ready: bool, have_all: bool) -> bool:
        if ready or not have_all:
            return False
        self.trees += 1
        return True


class GcRacesBuild(BlobStore):
    """agent.gc_trees before: a blob no tree links yet collected while another tree is being made from it."""

    # gdp: law no blob is collected while a tree is being made: not result or building == 0
    def drop_blob(self, links: int, age_s: int, many_trees: bool) -> bool:
        return links == 1 and age_s > 86400 and many_trees


class PullAnywhere(PullFile):
    """store.pull before: a manifest's path (or a link's) placed as given, `..` and all."""

    # gdp: law nothing is placed outside the pull's directory: not result or inside
    def place(self, inside: bool) -> bool:
        return True


class FileKeptOnMismatch(PullFile):
    """volumes' receiver before: a whole file PUT (op file) kept on disk though its sha256 differed (only the answer said so)."""

    # gdp: law a fetched file is cached only when its whole sha256 matches its digest: not result or got == digest
    def cache(self, digest: str, got: str) -> bool:
        return True


class RestoreTrivial(Checks):
    """volumes.restore_check before: a record without the manifest's sha256 compared the VM's with itself (passed)."""

    # gdp: law a restore passes only when every file and link matches and the manifest's sha256 is the one recorded: not result or (nbad == 0 and nbadlinks == 0 and files == want and recorded and sha_ok)
    def restore(self, nbad: int, nbadlinks: int, files: int, want: int, recorded: bool, sha_ok: bool) -> bool:
        return nbad == 0 and nbadlinks == 0 and files == want and (sha_ok or not recorded)


class MagicByAssert(Checks):
    """volumes.decrypt_check before: the SCV1 magic checked by an assert, gone under python -O."""

    # gdp: law a decryption passes only when every hash, the magic and the MAC match: not result or (cipher_ok and magic_ok and mac_ok and plain_ok and nbad == 0 and manifest_ok)
    def decrypted(self, cipher_ok: bool, magic_ok: bool, mac_ok: bool, plain_ok: bool, nbad: int, manifest_ok: bool) -> bool:
        return cipher_ok and mac_ok and plain_ok and nbad == 0 and manifest_ok


class CheckWithoutSentSha(CopyGates):
    """volumes.copy_one with modal_volumes's manifest_sha256 inside a comment: the copy's check without the sha256 Modal
    never sent (here as a default that compares nothing, the alternative to the KeyError every copy hit)."""

    # gdp: law a copy's check passes only when every file and link matches and its manifest's sha256 is the one sent: not result or (nbad == 0 and nbadlinks == 0 and files == want and want == sent and sha_given and sha_ok)
    def check(self, nbad: int, nbadlinks: int, files: int, want: int, sent: int, sha_given: bool, sha_ok: bool) -> bool:
        if self.state != "received" or not self._gate(nbad == 0 and nbadlinks == 0 and files == want and want == sent):
            return False
        self.checked = True
        self.state = "checked"
        return True


class TreeNeverGates(CopyGates):
    """volumes.copy_one before: the snapshot's tree checked and logged, never a gate."""

    # gdp: law a snapshot's tree passes only when every size and link it lists is as recorded: not result or (nbad == 0 and extra == 0 and nbadlinks == 0)
    def tree_ok(self, nbad: int, extra: int, nbadlinks: int) -> bool:
        if self.state != "snapshot":
            return False
        self.tree = True
        self.state = "tree"
        return True


class CleanupNoHost(Serving):
    """volumes.cleanup before: pid_alive(pid, "") is true for every pid (another host's, it says), so no ticketed VM
    was ever deleted."""

    # gdp: law a copy's VM goes once its client is dead: client_alive or result
    def cleanup(self, client_alive: bool) -> bool:
        return False


class PartsAnyOrder(PartOrder):
    """A receiver that consumes whichever part arrives (no index order)."""

    # gdp: law a part is consumed only in index order: next == old(next) or (i == old(next) and next == old(next) + 1)
    def consume(self, i: int) -> bool:
        if self.ended and self.next >= self.end:
            return False
        self.next += 1
        return True


class TreeOfAPreparedBatchDropped(BlobStore):
    """10-31 23:40-00:50: gc_trees kept the newest 6 trees and those of running items only; a long batch with nothing
    running at that moment lost its tree, the scheduler placed its items there on its word, each a row lost ("the
    batch's tree isn't on" the VM): 1,243 strikes, 153 items to Modal."""

    # gdp: law a tree a batch prepared here uses is never removed: not result or not prepared
    def drop_tree(self, rank: int, keep: int, in_use: bool, prepared: bool) -> bool:
        return rank >= keep and not in_use
