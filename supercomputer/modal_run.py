"""Removed execution backend; retained only for a clear error to legacy imports."""
from .config import MODAL_REMOVED

app = None


def modal_jobs(*args, **kwargs):
    raise ValueError(MODAL_REMOVED)
