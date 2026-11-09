"""Kept refutable owned-agent rules; formal gate checks each annotated entry on Boat."""

class BrokenRunnerReapIsNamespaceAbsence:
    """Kept bug: reaping a mount-only runner does not prove descendant teardown."""
    def __init__(self):
        self.namespace_absent = False

    # gdp: law a namespace proof requires containment: not namespace_absent or contained
    def observe(self, waited: bool, contained: bool):
        self.namespace_absent = waited  # broken: observe(true, false)


class BrokenFence:
    def __init__(self):
        self.cancelled = False

    def cancel(self):
        self.cancelled = True

    # gdp: law no launch after cancellation: not result or not cancelled
    def may_start(self) -> bool:
        return True  # broken: cancel; may_start
