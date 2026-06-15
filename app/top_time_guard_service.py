from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("forwarder")


class TopTimeGuardService:
    def __init__(self, repo, *, logger=None):
        self.repo = repo
        self.logger = logger or logging.getLogger("forwarder")

    def get_active_pause_for_rule_target(self, rule, *, at_iso: str | None = None) -> dict[str, Any] | None:
        target_id = str(getattr(rule, "target_id", "") or "")
        target_thread_id = getattr(rule, "target_thread_id", None)
        if not target_id:
            return None
        return self.repo.get_active_campaign_top_time_pause_for_target(
            target_id=target_id,
            target_thread_id=target_thread_id,
            at_iso=at_iso,
        )

    def build_guard_decision(self, rule, *, at_iso: str | None = None) -> dict[str, Any]:
        try:
            pause = self.get_active_pause_for_rule_target(rule, at_iso=at_iso)
        except Exception as exc:
            rule_id = getattr(rule, "id", None)
            target_id = str(getattr(rule, "target_id", "") or "")
            self.logger.warning(
                "TOP_TIME_GUARD_LOOKUP_FAILED | rule_id=%s | target_id=%s | error=%s",
                rule_id,
                target_id,
                exc,
            )
            return {"blocked": False, "reason": None, "pause": None, "resume_at": None}

        if not pause:
            return {"blocked": False, "reason": None, "pause": None, "resume_at": None}

        return {
            "blocked": True,
            "reason": "top_time_pause",
            "pause": pause,
            "resume_at": pause.get("ends_at"),
        }
