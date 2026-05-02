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


REACTION_ACCOUNT_REACTIONS_INPUT_STATE = "reaction_account_reactions_wait_input"


def is_reaction_account_reactions_input_state(user_states: dict[int, dict[str, Any]], user_id: int | None) -> bool:
    if user_id is None:
        return False
    state = user_states.get(int(user_id)) or {}
    return (
        state.get("state") == REACTION_ACCOUNT_REACTIONS_INPUT_STATE
        and state.get("flow") == "user_rule_reactions"
    )
