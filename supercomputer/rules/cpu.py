"""Exact agent-reported CPU eligibility and the per-constraint draw load guard."""


# gdp: law a handoff cannot discard live CPU constraints: not result or not required or supported
def cpu_handoff_allowed(required: bool, supported: bool) -> bool:
    return not required or supported


# gdp: law constrained placement has the exact model: not result or requested == "" or actual == requested
# gdp: law an unknown model cannot take constrained work: requested == "" or actual != "" or not result
# gdp: law unconstrained eligibility is unchanged: requested != "" or result
def cpu_matches(requested: str, actual: str) -> bool:
    return requested == "" or (actual != "" and requested == actual)


# gdp: law growth cannot exceed remaining fruitless draws: result + fruitless + pending <= limit or result == 0
# gdp: law growth never exceeds earned demand: result <= wanted
# gdp: law exhausted constraint cannot grow: fruitless + pending < limit or result == 0
def cpu_draws(wanted: int, fruitless: int, pending: int, limit: int) -> int:
    if wanted < 0 or fruitless < 0 or pending < 0 or limit < 1:
        raise ValueError("nonnegative counts and positive limit required")
    return min(wanted, max(0, limit - fruitless - pending))


# gdp: law CPU capacity never exceeds earned demand: result <= wanted
# gdp: law speculative capacity stays within remaining draws: result <= known + max(0, limit - fruitless - pending)
# gdp: law known matching capacity survives exhausted draws: known >= wanted or fruitless + pending < limit or result == known
# gdp: law sufficient known capacity needs no draw allowance: known < wanted or result == wanted
def cpu_capacity(wanted: int, known: int, fruitless: int, pending: int, limit: int) -> int:
    if wanted < 0 or known < 0 or fruitless < 0 or pending < 0 or limit < 1:
        raise ValueError("nonnegative counts and positive limit required")
    return min(wanted, known + max(0, limit - fruitless - pending))


# gdp: law only a verified known matching resume is exempt: result or (known_resume and expected != "" and actual == expected)
# gdp: law a verified known matching resume does not count: not known_resume or expected == "" or actual != expected or not result
# gdp: law an unknown or new draw always counts: known_resume or result
# gdp: law a mismatched or failed resume counts: expected != "" and actual == expected or result
def cpu_draw_counted(known_resume: bool, expected: str, actual: str) -> bool:
    return not known_resume or expected == "" or actual != expected


# gdp: law a matching draw resets the fruitless streak: not matched or result == 0
# gdp: law a fruitless draw advances the streak: matched or result == previous + 1
def cpu_streak(previous: int, matched: bool) -> int:
    if previous < 0:
        raise ValueError("nonnegative streak required")
    return 0 if matched else previous + 1
