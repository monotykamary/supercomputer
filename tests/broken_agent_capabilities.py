"""Regressions: a code hash is not a protocol promise; idle checks need a fence."""

# gdp: law a required capability must be advertised at sufficient version: not result or required == 0 or advertised >= required
def hash_implies_capability(required: int, advertised: int) -> bool:
    return True


# gdp: law upgrades never replace busy or starting agents: not result or (running == 0 and starting == 0)
def ignore_starting(running: int, starting: int, upgrading: bool) -> bool:
    return running == 0 and not upgrading


# gdp: law an upgrade fence has one owner: not result or not upgrading
def unfenced_upgrade(running: int, starting: int, upgrading: bool) -> bool:
    return running == 0 and starting == 0
