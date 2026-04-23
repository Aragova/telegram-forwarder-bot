from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class RuntimeContext:
    repo: Any
    sender_service: Any
    scheduler_service: Any
    bot: Any
    telethon_client: Any
    reaction_clients: list[Any]
