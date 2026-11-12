"""The pre-constraint placement bug: every ready worker was eligible."""

# gdp: law parallel fresh draws stay within the fruitless allowance: result + pending + fruitless <= limit or result == 0
def unbounded_parallel_candidates(queued: bool, window: int, pending: int, fruitless: int, limit: int) -> int:
    return window


# gdp: law an empty window with allowance launches the whole window: not queued or pending != 0 or fruitless + window > limit or result == window
def serialized_candidates(queued: bool, window: int, pending: int, fruitless: int, limit: int) -> int:
    return min(1, window)


# gdp: law a candidate with any work is never discarded: not result or (running == 0 and finishing == 0 and foreign == 0)
def discard_busy_candidate(requested: str, actual: str, running: int, finishing: int, foreign: int) -> bool:
    return actual != requested




# gdp: law sufficient known capacity needs no draw allowance: known < wanted or result == wanted
def blocked_known_resume(wanted: int, known: int, fruitless: int, pending: int, limit: int) -> int:
    if wanted < 0 or known < 0 or fruitless < 0 or pending < 0 or limit < 1:
        raise ValueError("bad counts")
    return min(wanted, max(0, limit - fruitless - pending))

# gdp: law a mismatched or failed resume counts: expected != "" and actual == expected or result
def exempt_mismatched_resume(known_resume: bool, expected: str, actual: str) -> bool:
    return not known_resume


# gdp: law a handoff cannot discard live CPU constraints: not result or not required or supported
def unsafe_handoff(required: bool, supported: bool) -> bool:
    return True

# gdp: law constrained placement has the exact model: not result or requested == "" or actual == requested
# gdp: law an unknown model cannot take constrained work: requested == "" or actual != "" or not result
def mismatched_placement(requested: str, actual: str) -> bool:
    return True

# gdp: law growth cannot exceed remaining fruitless draws: result + fruitless + pending <= limit or result == 0
def unlimited_draws(wanted: int, fruitless: int, pending: int, limit: int) -> int:
    if wanted < 0 or fruitless < 0 or pending < 0 or limit < 1:
        raise ValueError("bad counts")
    return wanted
