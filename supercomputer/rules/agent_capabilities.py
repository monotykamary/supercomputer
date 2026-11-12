"""Agent protocol requirements and the idle-only upgrade admission fence."""


# gdp: law a required capability must be advertised at sufficient version: not result or required == 0 or advertised >= required
# gdp: law undeclared capability does not constrain placement: required != 0 or result
def agent_capability_satisfies(required: int, advertised: int) -> bool:
    if required < 0 or advertised < 0:
        raise ValueError("nonnegative protocol versions required")
    return required == 0 or advertised >= required


# gdp: law upgrades never replace busy or starting agents: not result or (running == 0 and starting == 0)
# gdp: law an upgrade fence has one owner: not result or not upgrading
def agent_upgrade_allowed(running: int, starting: int, upgrading: bool) -> bool:
    if running < 0 or starting < 0:
        raise ValueError("nonnegative run counts required")
    return running == 0 and starting == 0 and not upgrading


def normalize_agent_capabilities(value) -> dict[str, int]:
    """Untrusted ping inventory: malformed/unknown values grant no capability."""
    if not isinstance(value, dict):
        return {}
    return {k: v for k, v in value.items()
            if isinstance(k, str) and k.strip() and type(v) is int and v > 0}


def required_agent_capabilities(spec) -> dict[str, int]:
    """Validate explicit requirements and derive protocols from declared fields.

    Runtime pins are never inferred from command text. Import their single schema
    lazily so ordinary jobs do not depend on runtime inspection machinery.
    """
    value = spec.get("required_agent_capabilities", {})
    if not isinstance(value, dict) or any(not isinstance(k, str) or not k.strip()
            or type(v) is not int or v < 1 for k, v in value.items()):
        raise ValueError("required_agent_capabilities must map nonblank names to positive integer versions")
    required = dict(value)
    from ..agent import normalize_memory_limit
    if normalize_memory_limit(spec) is not None:
        required["resource_limits_protocol"] = max(1, required.get("resource_limits_protocol", 0))
    if "runtime" in spec:
        from ..agent import normalize_runtime_pins
        normalize_runtime_pins(spec["runtime"])
        required["runtime_pins_protocol"] = max(1, required.get("runtime_pins_protocol", 0))
    return required


def agent_capabilities_match(required, advertised) -> bool:
    inventory = normalize_agent_capabilities(advertised)
    return all(agent_capability_satisfies(version, inventory.get(name, 0))
               for name, version in required.items())
