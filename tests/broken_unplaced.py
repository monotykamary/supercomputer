"""The 11-08 planner's imaginary foreign capacity and repeated missing-VM resumes."""
# gdp: law an exclusive item cannot share foreign work: not timing or foreign == 0 or result == 0
def foreign_capacity(slots: int, foreign: int, timing: bool) -> int:
    return slots


# gdp: law a confirmed missing resume is replaced: not missing or result
def missing_resume(missing: bool, expired: bool) -> bool:
    return expired


# gdp: law unserved exclusive batches have a lane: result + reusable + coming >= waiting
def guessed_lane(waiting: int, reusable: int, coming: int) -> int:
    return 0
