"""Fail-closed runtime verification gate (also embedded in the standalone agent)."""


class RuntimePinGate:
    # gdp: law missing runtimes never launch: not result or present
    # gdp: law non executable runtimes never launch: not result or executable
    # gdp: law mismatching hashes never launch: not result or hash_ok
    # gdp: law mismatching versions never launch: not result or version_ok
    # gdp: law changed runtimes never launch: not result or stable
    # gdp: law complete verification permits launch: not (present and executable and hash_ok and version_ok and stable) or result
    def allow(self, present: bool, executable: bool, hash_ok: bool, version_ok: bool, stable: bool) -> bool:
        return present and executable and hash_ok and version_ok and stable
