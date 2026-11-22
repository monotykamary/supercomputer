"""What `uv tool install git+https://github.com/monotykamary/supercomputer` installs since 1.0: a pointer."""
import sys

MESSAGE = """supercomputer 1.0 is TypeScript on instabox; this Python package no longer runs anything.

Install it:
  git clone https://github.com/monotykamary/supercomputer && cd supercomputer && ./install.sh
  (it needs bun, https://bun.sh, and a signed-in instabox: `instabox login`)
  then remove this one: uv tool uninstall supercomputer

The Python supercomputer (boat, Modal, treadmill, RunPod) is pinned at v0.1.0-python:
  uv tool install git+https://github.com/monotykamary/supercomputer@v0.1.0-python
"""


def main() -> int:
    sys.stderr.write(MESSAGE)
    return 1


if __name__ == "__main__":
    sys.exit(main())
