"""Deliberate regression: capability lookup followed by unguarded legacy dispatch."""
# gdp: law owned RPC requires exact protocol and source: not result or (protocol and version)
# gdp: law owned RPC requires fresh nonce and actual features: not result or (nonce and features)
# gdp: law replacement fence closes owned dispatch: not result or not upgrading
def broken_owned_rpc_allowed(protocol: bool, version: bool, nonce: bool, features: bool, upgrading: bool) -> bool:
    return True
