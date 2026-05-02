from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ReactionRuntimePlan:
    mode: str
    tenant_id: int | None
    use_legacy_reactors: bool
    use_tenant_reactors: bool
    reason: str
    tenant_accounts: list[dict[str, Any]] = field(default_factory=list)


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
        if not accounts:
            return ReactionRuntimePlan(
                mode="no_accounts",
                tenant_id=tenant_id,
                use_legacy_reactors=False,
                use_tenant_reactors=False,
                reason="no_active_tenant_reaction_accounts",
            )

        return ReactionRuntimePlan(
            mode="tenant_saas",
            tenant_id=tenant_id,
            use_legacy_reactors=False,
            use_tenant_reactors=True,
            reason="tenant_reactions_enabled",
            tenant_accounts=accounts,
        )
