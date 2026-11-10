"""Pool growth: marginal work pays for starts; a gate buys its deadline instead.

The small predicates are the proved decisions. The arithmetic wrappers count
identical VMs (floor for paid-for work, ceiling for a deadline), tested against
those predicates at the boundary. All work is slot-seconds, not item counts.
"""
import math


def break_even_s(hold_s: float, start_dollars: float, vm_hour_dollars: float, vm_multiplier: float = 1.0) -> float:
    """Recover the larger of a start and the hold's default-VM opportunity cost.

    Billing and the hold's economic baseline use default-VM hours. A large VM
    buys twice that capacity per second: normalize the money, not the wall time.
    Free starts and prepaid hours still consume shared allowances.
    """
    if not all(math.isfinite(x) for x in (hold_s, start_dollars, vm_hour_dollars, vm_multiplier)) or hold_s < 0 or start_dollars < 0 or vm_hour_dollars <= 0 or vm_multiplier <= 0 or (hold_s == 0 and start_dollars == 0):
        raise ValueError("growth requires finite nonnegative hold/start price, positive VM-hour price and positive break-even")
    return max(hold_s, 3600.0 * start_dollars / vm_hour_dollars) / vm_multiplier


# gdp: law no needless start when held and coming capacity clears the queue: work > capacity or not result
# gdp: law every economic start has a full VM of marginal work: not result or work - capacity >= vm_work
# gdp: law a full VM of marginal work earns a start: work - capacity < vm_work or result
def economic_start(work: float, capacity: float, vm_work: float) -> bool:
    if work < 0 or capacity < 0 or vm_work <= 0:
        raise ValueError("nonnegative work/capacity and positive VM work required")
    return work - capacity >= vm_work


# gdp: law a gate never waits past its horizon for want of a start: work <= capacity or result
# gdp: law a gate within its horizon needs no start: work > capacity or not result
def gate_start(work: float, capacity: float) -> bool:
    if work < 0 or capacity < 0:
        raise ValueError("nonnegative work and capacity required")
    return work > capacity


def economic_vms(work: float, capacity: float, vm_work: float) -> int:
    """Only whole marginal VM workloads buy starts; never ceil a partial one."""
    if not economic_start(work, capacity, vm_work):
        return 0
    return math.floor((work - capacity) / vm_work)


# gdp: law gate planning uses the remaining deadline whenever a new item can finish: remaining < bring_up + item_s or result == remaining
# gdp: law an indivisible gate item has time to start and finish: result >= bring_up + item_s
def gate_window(remaining: float, bring_up: float, item_s: float) -> float:
    if remaining < 0 or bring_up < 0 or item_s < 0:
        raise ValueError("nonnegative gate times required")
    return max(remaining, bring_up + item_s)

def gate_vms(work: float, capacity: float, vm_work: float) -> int:
    """The minimum additional capacity which clears the gate by its deadline."""
    if vm_work <= 0:
        raise ValueError("positive gate VM work required")
    if not gate_start(work, capacity):
        return 0
    return math.ceil((work - capacity) / vm_work)
