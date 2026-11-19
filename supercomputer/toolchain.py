"""Compare native launch toolchains independently of job exit codes or probe policy."""
import hashlib
import json


def _fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


class BatchToolchains:
    def __init__(self):
        self.jobs = 0
        self.unmeasured = 0
        self.refused = 0
        self.boots = set()
        self.paths = set()
        self.profiles = {}
        self.versions = {}
        self.hashes = {}

    def observe(self, row):
        self.jobs += 1
        receipt = row.get("toolchain")
        if isinstance(receipt, dict) and receipt.get("ok") is False:
            self.refused += 1
        if (not isinstance(receipt, dict) or receipt.get("protocol") != 1 or not isinstance(receipt.get("path"), str)
                or receipt.get("path_truncated")):
            self.unmeasured += 1
            return
        self.paths.add(_fingerprint(receipt["path"]))
        if receipt.get("shell_startup") or receipt.get("shell_functions"):
            self.unmeasured += 1
            return
        observed, required = receipt.get("observed"), receipt.get("required")
        if (not isinstance(observed, dict) or not isinstance(required, dict) or "node" not in observed
                or not required.keys() <= observed.keys() or any(not isinstance(entry, dict) for entry in observed.values())):
            self.unmeasured += 1
            return
        tools = {}
        incomplete = False
        for name, entry in observed.items():
            status = entry.get("status")
            if status in ("observed", "version_mismatch", "missing"):
                tools[name] = [entry.get(key) for key in ("path", "realpath", "version")]
                self.versions.setdefault(name, set()).add(_fingerprint(tools[name]))
            if isinstance(entry.get("sha256"), str):
                self.hashes.setdefault(name, set()).add(entry["sha256"])
            if name not in tools or (status == "missing" and name in required):
                incomplete = True
        self.unmeasured += int(incomplete)
        # All new workers measure Node (including absence). Optional declarations
        # are compared independently when measured, not treated as differences
        # just because another job did not ask for a probe or hash.
        if "node" in tools:
            key = _fingerprint([receipt["path"], tools["node"]])
            boot = receipt.get("boot_id")
            if isinstance(boot, str) and boot:
                self.boots.add(boot)
            sample = self.profiles.setdefault(key, {"jobs": 0, "example": row.get("name"), "boot_id": boot})
            sample["jobs"] += 1

    def summary(self):
        different = sorted({name for probes in (self.versions, self.hashes)
                            for name, values in probes.items() if len(values) > 1})
        mixed = len(self.paths) > 1 or bool(different)
        return {"status": ("mixed" if mixed else "pin_refused" if self.refused else
                           "incomplete" if self.unmeasured or not self.jobs else "consistent"),
                "mixed": mixed, "jobs": self.jobs, "unmeasured": self.unmeasured, "pin_refused": self.refused,
                "boots_observed": len(self.boots), "different_paths": len(self.paths) > 1,
                "fingerprints": self.profiles, "different_tools": different}
