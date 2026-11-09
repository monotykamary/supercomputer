"""The local content cache is disposable, not an evidence archive."""


class CompletionPin:
    # gdp: law unfinished jobs keep their cache pins: not result or pending == 0
    # gdp: law completion cannot release another generation: not result or generation_matches
    # gdp: law matching finished jobs release their pins: result or pending != 0 or not generation_matches
    def release(self, pending: int, generation_matches: bool) -> bool:
        return pending == 0 and generation_matches


class BlobEviction:
    # gdp: law a pinned blob is never removed: not result or not pinned
    # gdp: law a recently used blob is never removed: not result or not recent
    # gdp: law removal needs pressure or expiry: not result or total > budget or expired
    # gdp: law eligible cold blobs are reclaimed: result or pinned or recent or (total <= budget and not expired)
    def remove(self, pinned: bool, recent: bool, total: int, budget: int, expired: bool) -> bool:
        return not pinned and not recent and (total > budget or expired)
