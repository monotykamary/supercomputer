"""supercomputer's decision rules, apart from threads, I/O and the providers' APIs: each module's classes and functions
decide and record, the code calls them, and each holds the laws stated in it (`# gdp: law`), proven by gdp-wired from
every state the laws allow (`tests/check_laws.sh`). COVERAGE.md lists every function of the tool: in a rule here with its
laws, or I/O and why.

    scheduler   an item's fate, the pool's starts and breaker, a VM's owner, hold and release, policy generations,
                the watchdog, the handoff, acquisition, placement, preemption
"""
from .scheduler import *  # noqa: F401,F403
from .growth import break_even_s, economic_start, economic_vms, gate_start, gate_vms, gate_window  # noqa: F401
from .updates import *  # noqa: F401,F403
