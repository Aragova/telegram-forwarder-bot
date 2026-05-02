from __future__ import annotations

from typing import Any

REACTION_AUTH_STATES = {
    "reaction_auth_wait_phone",
    "reaction_auth_wait_code",
    "reaction_auth_wait_password",
}


def is_reaction_auth_state(user_states: dict[int, dict[str, Any]], user_id: int | None) -> bool:
    if user_id is None:
        return False
    state = user_states.get(int(user_id)) or {}
    return state.get("flow") == "user_rule_reactions" and state.get("state") in REACTION_AUTH_STATES
