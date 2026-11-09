"""Keep the unsafe cache policies refuted by the law gate."""


class ReleasesRunning:
    # gdp: law unfinished jobs keep their cache pins: not result or pending == 0
    def release(self, pending: int, generation_matches: bool) -> bool:
        return generation_matches


class ReleasesReusedID:
    # gdp: law completion cannot release another generation: not result or generation_matches
    def release(self, pending: int, generation_matches: bool) -> bool:
        return pending == 0


class DropsPinned:
    # gdp: law a pinned blob is never removed: not result or not pinned
    def remove(self, pinned: bool, recent: bool, total: int, budget: int, expired: bool) -> bool:
        return not recent and (total > budget or expired)


class DropsRecent:
    # gdp: law a recently used blob is never removed: not result or not recent
    def remove(self, pinned: bool, recent: bool, total: int, budget: int, expired: bool) -> bool:
        return not pinned and (total > budget or expired)


class DropsWithoutPressure:
    # gdp: law removal needs pressure or expiry: not result or total > budget or expired
    def remove(self, pinned: bool, recent: bool, total: int, budget: int, expired: bool) -> bool:
        return not pinned and not recent
