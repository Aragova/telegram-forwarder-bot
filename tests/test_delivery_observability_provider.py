from __future__ import annotations

import ast
import asyncio
from pathlib import Path

from app.delivery_observability import DeliveryHealthStatus, DeliveryObservabilityService, DeliveryRuleMetrics
from app.delivery_observability_provider import RepositoryDeliveryObservabilityProvider

ROOT = Path(__file__).resolve().parents[1]


class FakeRepository:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_delivery_observability_rule_metrics(self):
        self.calls.append("get_delivery_observability_rule_metrics")
        return [
            {
                "rule_id": 10,
                "source_id": "src",
                "target_id": "tgt",
                "pending_count": 2,
                "processing_count": 1,
                "sent_count": 5,
                "faulty_count": 0,
            }
        ]


class BrokenRepository:
    def get_delivery_observability_rule_metrics(self):
        raise RuntimeError("SECRET_TOKEN failure")


def test_provider_calls_only_read_only_repository_method_and_returns_metrics() -> None:
    repo = FakeRepository()
    provider = RepositoryDeliveryObservabilityProvider(repo)

    result = asyncio.run(provider.get_delivery_rule_metrics())

    assert repo.calls == ["get_delivery_observability_rule_metrics"]
    assert isinstance(result[0], DeliveryRuleMetrics)
    assert result[0].rule_id == 10
    assert result[0].pending_count == 2


def test_repository_error_becomes_safe_unknown_snapshot() -> None:
    provider = RepositoryDeliveryObservabilityProvider(BrokenRepository())
    service = DeliveryObservabilityService(metrics_provider=provider)

    snapshot = asyncio.run(service.collect_snapshot())

    assert snapshot.status == DeliveryHealthStatus.UNKNOWN
    assert "metrics_provider_failed" in (snapshot.reason or "")
    assert "SECRET_TOKEN" not in (snapshot.reason or "")


def test_provider_has_no_runtime_imports() -> None:
    source = (ROOT / "app/delivery_observability_provider.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")

    forbidden = ("bot", "app.sender", "app.worker_runtime", "app.video_processor", "VideoProcessor")
    assert all(marker not in source for marker in forbidden)
    assert all(module not in forbidden for module in imported)
