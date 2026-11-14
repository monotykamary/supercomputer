"""Deliberate regression: capability lookup followed by unguarded legacy dispatch."""
# gdp: law owned RPC requires exact protocol and source: not result or (protocol and version)
# gdp: law owned RPC requires fresh nonce and actual features: not result or (nonce and features)
# gdp: law replacement fence closes owned dispatch: not result or not upgrading
def broken_owned_rpc_allowed(protocol: bool, version: bool, nonce: bool, features: bool, upgrading: bool) -> bool:
    return True


# gdp: law a fresh execution requires permission and no prior receipt: result != 'execute' or (not found and permitted)
# gdp: law a replay requires the same completed request and scope: result != 'replay' or (found and same_request and same_scope and complete)
def broken_owned_rpc_decision(found: bool, same_request: bool, same_scope: bool, complete: bool, permitted: bool):
    return 'replay' if found else 'execute'
