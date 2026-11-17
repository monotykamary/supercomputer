"""Advisory for unexpectedly broad exported job outputs (never a validation error).

Native jobs declare only name/cmd/etc.; they export files by writing to
$SUPERCOMPUTER_OUTPUT_DIR. The optional declaration keys below are accepted
by this helper for callers with their own job metadata, not by the native schema.
"""

import json
import re
import shlex
import sys

_FIELDS = ("output", "output_glob", "output_globs", "out_dir", "output_dir")
_DIRS = frozenset((".lake", "build", "_build", ".build", "dist", ".next", "node_modules", "target"))


def _broad(path):
    if not isinstance(path, str):
        return False
    parts = path.strip().replace("\\", "/").rstrip("/").split("/")
    while parts and parts[0] == ".":
        parts.pop(0)
    while parts and parts[-1] == ".":
        parts.pop()
    if not parts:
        return True  # . or ./
    if parts == ["**"] or parts == ["*"] or parts == ["**", "*"]:
        return True
    if parts[0] == "$SUPERCOMPUTER_OUTPUT_DIR" or parts[0] == "${SUPERCOMPUTER_OUTPUT_DIR}":
        return False  # destination of the export, not a source selection
    if parts[-1] in ("*", "**") and parts[:-1] == ["**"]:
        return True
    for index, part in enumerate(parts):
        if part in _DIRS and (index == len(parts) - 1 or
                              all(p in ("*", "**") for p in parts[index + 1:])):
            return True
    return False


def _values(value):
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple)):
        return tuple(v for v in value if isinstance(v, str))
    return ()


def _command_source(cmd):
    """Narrow native-schema fallback: recursive copy of a broad source into export dir.

    Do not infer an output from a mere mention of a build directory (e.g. rm,
    echo, compiler commands). This is intentionally not a shell parser.
    """
    if not isinstance(cmd, str) or "SUPERCOMPUTER_OUTPUT_DIR" not in cmd:
        return None
    for segment in re.split(r"(?:&&|;|\n)", cmd):
        try:
            tokens = shlex.split(segment)
        except ValueError:
            continue
        if not tokens or tokens[0] not in ("cp", "rsync"):
            continue
        args = tokens[1:]
        if len(args) < 3 or not any("SUPERCOMPUTER_OUTPUT_DIR" in a for a in args[-1:]):
            continue
        flags = [a for a in args[:-1] if a.startswith("-")]
        if tokens[0] == "cp" and not any("r" in f or "R" in f or "a" in f for f in flags):
            continue
        if tokens[0] == "rsync" and not any("a" in f or "r" in f for f in flags):
            continue
        for source in args[:-1]:
            if not source.startswith("-") and _broad(source):
                return source
    return None


def warn_broad_outputs(job, *, stream=None):
    """Emit at most one one-line stderr WARNING for a job; return whether emitted.

    Call once per job at submission, after parsing jobs.json and before launching
    jobs (for CLI run: __main__._run, before fanout.run_jobs). Do not pass --out:
    it is the local results collector, not an exported job output declaration.
    """
    if stream is None:
        stream = sys.stderr
    match = None
    declared = any(key in job for key in _FIELDS)
    for key in _FIELDS:
        for path in _values(job.get(key)):
            if _broad(path):
                match = (key, path)
                break
        if match:
            break
    if match is None and not declared:
        source = _command_source(job.get("cmd"))
        if source is not None:
            match = ("cmd copy source", source)
    if match is None:
        return False
    name = json.dumps(str(job.get("name", "<unnamed>")), ensure_ascii=True)
    detail = json.dumps(match[1], ensure_ascii=True)
    stream.write(f"WARNING: job {name} {match[0]}={detail} looks like a whole-tree output; export only needed files.\n")
    return True
