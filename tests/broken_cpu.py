"""The pre-constraint placement bug: every ready worker was eligible."""

# gdp: law constrained placement has the exact model: not result or requested == "" or actual == requested
# gdp: law an unknown model cannot take constrained work: requested == "" or actual != "" or not result
def mismatched_placement(requested: str, actual: str) -> bool:
    return True

# gdp: law growth cannot exceed remaining fruitless draws: result + fruitless + pending <= limit or result == 0
def unlimited_draws(wanted: int, fruitless: int, pending: int, limit: int) -> int:
    if wanted < 0 or fruitless < 0 or pending < 0 or limit < 1:
        raise ValueError("bad counts")
    return wanted
