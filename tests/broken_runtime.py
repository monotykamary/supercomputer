"""Unsafe runtime verification variants kept for refutation."""


class IgnoreHash:
    # gdp: law mismatching hashes never launch: not result or hash_ok
    def allow(self, present: bool, executable: bool, hash_ok: bool, version_ok: bool, stable: bool) -> bool:
        return present and executable and version_ok and stable


class IgnoreVersion:
    # gdp: law mismatching versions never launch: not result or version_ok
    def allow(self, present: bool, executable: bool, hash_ok: bool, version_ok: bool, stable: bool) -> bool:
        return present and executable and hash_ok and stable


class IgnoreReplacement:
    # gdp: law changed runtimes never launch: not result or stable
    def allow(self, present: bool, executable: bool, hash_ok: bool, version_ok: bool, stable: bool) -> bool:
        return present and executable and hash_ok and version_ok
