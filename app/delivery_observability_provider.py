"""Read-only repository adapter for delivery observability metrics."""

from __future__ import annotations

import asyncio
from inspect import isawaitable, iscoroutinefunction
from typing import Any

from app.delivery_observability import DeliveryRuleMetrics, normalize_delivery_rule_metrics


class RepositoryDeliveryObservabilityProvider:
    """Load delivery diagnostics metrics from a repository using a read-only method."""

    def __init__(self, repository: Any) -> None:
        self.repository = repository

    async def get_delivery_rule_metrics(self) -> tuple[DeliveryRuleMetrics, ...]:
        method = getattr(self.repository, "get_delivery_observability_rule_metrics")
        if iscoroutinefunction(method):
            result = await method()
        else:
            result = await asyncio.to_thread(method)
        if isawaitable(result):
            result = await result
        return tuple(normalize_delivery_rule_metrics(item) for item in (result or ()))
