"""Terminal ownership regressions refused by the production takeover laws."""


# gdp: law every exact open terminal hold is retained: not (strict and terminal and matched and not closed) or result
def drop_terminal_hold(strict: bool, terminal: bool, matched: bool, closed: bool) -> bool:
    return False


# gdp: law terminal holds require exact persisted identity: not result or matched
def trust_offered_identity(strict: bool, terminal: bool, matched: bool, closed: bool) -> bool:
    return strict and terminal and not closed


# gdp: law closed admissions never become live cleanup work: not result or not closed
def resurrect_closed_hold(strict: bool, terminal: bool, matched: bool, closed: bool) -> bool:
    return strict and terminal and matched


# gdp: law a persisted terminal item never reopens on takeover: not terminal or result == "done"
def reopen_terminal(terminal: bool, offered: str) -> str:
    return offered
