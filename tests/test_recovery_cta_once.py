from __future__ import annotations

import bot


class _Repo:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def get_billing_events(self, tenant_id: int, limit: int = 20):
        rows = [row for row in self.events if int(row.get("tenant_id") or 0) == int(tenant_id)]
        return list(reversed(rows))[: int(limit)]

    def create_billing_event(self, tenant_id: int, event_type: str, *, event_source=None, metadata=None, **kwargs):
        self.events.append(
            {
                "tenant_id": int(tenant_id),
                "event_type": str(event_type),
                "event_source": event_source,
                "metadata_json": metadata or {},
            }
        )


def test_mark_recovery_cta_shown_and_detect_once() -> None:
    repo = _Repo()
    original_db = bot.db
    bot.db = repo
    try:
        assert bot._is_recovery_cta_already_shown(tenant_id=7, payment_intent_id=101) is False
        bot._mark_recovery_cta_shown(tenant_id=7, payment_intent_id=101, user_id=500)
        assert bot._is_recovery_cta_already_shown(tenant_id=7, payment_intent_id=101) is True
        assert bot._is_recovery_cta_already_shown(tenant_id=7, payment_intent_id=102) is False
    finally:
        bot.db = original_db
