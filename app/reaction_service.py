from __future__ import annotations

from pathlib import Path
from typing import Any


class ReactionService:
    """
    SaaS reaction service skeleton.

    ВАЖНО:
    - legacy REACTION_SESSIONS из .env пока остаются в app/telegram_client.py / SenderService;
    - этот сервис готовит будущий tenant-scoped runtime;
    - все аккаунты-реакторы должны выбираться строго по tenant_id;
    - глобальные аккаунты владельца сервиса нельзя использовать для чужих tenant.
    """

    def __init__(self, db):
        self.db = db

    def get_session_dir_for_tenant(self, tenant_id: int) -> Path:
        """
        Session files для SaaS-режима должны быть изолированы по tenant:
        sessions/tenants/<tenant_id>/reactors/<account_id>.session
        """
        session_dir = Path("sessions") / "tenants" / str(tenant_id) / "reactors"
        session_dir.mkdir(parents=True, exist_ok=True)
        return session_dir

    def select_reaction_accounts_for_tenant(self, tenant_id: int, *, active_only: bool = True) -> list[dict[str, Any]]:
        return self.db.list_reaction_accounts_for_tenant(tenant_id=tenant_id, active_only=active_only)

    def enqueue_reaction_jobs_for_delivery(
        self,
        *,
        tenant_id: int,
        rule_id: int | None,
        delivery_id: int | None,
        target_id: str,
        message_id: int,
        reaction_payload: dict[str, Any],
    ) -> list[int]:
        """
        Foundation-only hook.
        Текущий runtime SenderService в этом PR не переводится на reaction_jobs.
        """
        job_id = self.db.enqueue_reaction_job(
            tenant_id=tenant_id,
            rule_id=rule_id,
            delivery_id=delivery_id,
            target_id=target_id,
            message_id=message_id,
            account_id=None,
            reaction_payload=reaction_payload,
        )
        return [job_id] if job_id is not None else []
