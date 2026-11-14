"""Hash the complete installer into Boat's native snapshot identity."""
from pathlib import Path


def layers(root):
    # root is the shipped repository; no checkout-specific absolute path is hashed.
    yield "toolchains", (Path(__file__).parent / "install.sh").read_text()
