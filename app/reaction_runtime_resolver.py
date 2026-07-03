from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any


logger = logging.getLogger("forwarder")


@dataclass
class ReactionRuntimePlan:
    mode: str
    tenant_id: int | None
    use_legacy_reactors: bool
    use_tenant_reactors: bool
    reason: str
    tenant_accounts: list[dict[str, Any]] = field(default_factory=list)
    eligible_accounts: int = 0
    selected_accounts: int = 0
    skipped_accounts: int = 0
    limit_applied: bool = False
    limit: int | None = None
    selection_reason: str | None = None


class ReactionRuntimeResolver:
    def __init__(self, db):
        self.db = db

    def resolve_for_rule(self, rule) -> ReactionRuntimePlan:
        rule_id = int(getattr(rule, "id", 0) or 0)

        tenant_id = None
        if hasattr(rule, "tenant_id"):
            try:
                tenant_id = int(getattr(rule, "tenant_id") or 0)
            except Exception:
                tenant_id = None

        if tenant_id in (None, 0) and hasattr(self.db, "get_rule_tenant_id") and rule_id > 0:
            try:
                tenant_id = int(self.db.get_rule_tenant_id(rule_id) or 0)
            except Exception:
                tenant_id = None

        if tenant_id is None or tenant_id <= 1:
            return ReactionRuntimePlan(
                mode="legacy_admin",
                tenant_id=tenant_id,
                use_legacy_reactors=True,
                use_tenant_reactors=False,
                reason="admin_or_legacy_rule",
            )

        settings = self.db.get_rule_reaction_settings_for_tenant(tenant_id, rule_id)
        if not settings or not bool(settings.get("enabled")):
            return ReactionRuntimePlan(
                mode="disabled",
                tenant_id=tenant_id,
                use_legacy_reactors=False,
                use_tenant_reactors=False,
                reason="tenant_reactions_disabled",
            )

        accounts = self.db.list_reaction_accounts_for_tenant(tenant_id=tenant_id, active_only=True) or []
        eligible_accounts = len(accounts)
        if not accounts:
            return ReactionRuntimePlan(
                mode="no_accounts",
                tenant_id=tenant_id,
                use_legacy_reactors=False,
                use_tenant_reactors=False,
                reason="no_active_tenant_reaction_accounts",
                eligible_accounts=0,
                selected_accounts=0,
                skipped_accounts=0,
                selection_reason="no_active_tenant_reaction_accounts",
            )

        logger.info(
            "REACTION_ACCOUNT_SELECTION | rule_id=%s | tenant_id=%s | mode=tenant_saas | eligible_accounts=%s | selected_accounts=%s | skipped_accounts=%s | limit_applied=%s | limit=%s | reason=%s",
            rule_id,
            tenant_id,
            eligible_accounts,
            len(accounts),
            max(eligible_accounts - len(accounts), 0),
            False,
            None,
            "no_limit_all_active_accounts",
        )

        return ReactionRuntimePlan(
            mode="tenant_saas",
            tenant_id=tenant_id,
            use_legacy_reactors=False,
            use_tenant_reactors=True,
            reason="tenant_reactions_enabled",
            tenant_accounts=accounts,
            eligible_accounts=eligible_accounts,
            selected_accounts=len(accounts),
            skipped_accounts=max(eligible_accounts - len(accounts), 0),
            limit_applied=False,
            limit=None,
            selection_reason="no_limit_all_active_accounts",
        )
