from __future__ import annotations

import logging
import os
from typing import Iterable

from .sender_pipeline_rollout import (
    SenderPipelineName,
    SenderPipelineRolloutConfig,
    SenderPipelineRolloutStrategy,
    SenderRolloutMode,
)

logger = logging.getLogger("forwarder")

_MODE_ENV = "SENDER_PIPELINE_ROLLOUT_MODE"
_RULE_IDS_ENV = "SENDER_PIPELINE_ROLLOUT_REPOST_SINGLE_RULE_IDS"
_BLOCKED_RULE_IDS_ENV = "SENDER_PIPELINE_ROLLOUT_BLOCKED_RULE_IDS"
_ACTIVE_CANARY_ENV = "SENDER_PIPELINE_REPOST_SINGLE_ACTIVE_CANARY_ENABLED"


def repost_single_active_canary_enabled_from_env() -> bool:
    return _parse_bool(os.getenv(_ACTIVE_CANARY_ENV, ""))


def _parse_bool(raw: str | None) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}


def build_repost_single_active_canary_config_from_env():
    from .repost_single_active_canary import RepostSingleActiveCanaryConfig

    return RepostSingleActiveCanaryConfig(
        canary_enabled=repost_single_active_canary_enabled_from_env(),
        enabled_rule_ids=_parse_rule_ids(os.getenv(_RULE_IDS_ENV, "")),
    )


def build_sender_pipeline_rollout_strategy_from_env() -> SenderPipelineRolloutStrategy:
    """Build fail-closed Stage 27 rollout strategy from environment."""
    raw_mode = os.getenv(_MODE_ENV, "disabled").strip().lower() or "disabled"
    rule_ids = _parse_rule_ids(os.getenv(_RULE_IDS_ENV, ""))
    blocked_rule_ids = _parse_rule_ids(os.getenv(_BLOCKED_RULE_IDS_ENV, ""))

    if raw_mode not in {"disabled", "dry_run", "shadow", "active"}:
        logger.warning(
            "SENDER_ROLLOUT | unknown mode ignored | mode=%s | fallback=disabled",
            raw_mode,
        )
        raw_mode = "disabled"

    config_kwargs = {
        "mode": SenderRolloutMode.DISABLED,
        "blocked_rule_ids": blocked_rule_ids,
        "rollout_percent": 0,
        "require_rule_allowlist": True,
        "fail_closed": True,
    }

    if raw_mode == "dry_run":
        config_kwargs.update(
            mode=SenderRolloutMode.DRY_RUN,
            dry_run_pipelines=(SenderPipelineName.REPOST_SINGLE,),
            dry_run_rule_ids=rule_ids,
        )
    elif raw_mode == "shadow":
        config_kwargs.update(
            mode=SenderRolloutMode.SHADOW,
            shadow_pipelines=(SenderPipelineName.REPOST_SINGLE,),
            shadow_rule_ids=rule_ids,
        )
    elif raw_mode == "active":
        config_kwargs.update(
            mode=SenderRolloutMode.ACTIVE,
            enabled_pipelines=(SenderPipelineName.REPOST_SINGLE,),
            enabled_rule_ids=rule_ids,
        )

    return SenderPipelineRolloutStrategy(config=SenderPipelineRolloutConfig(**config_kwargs))


def _parse_rule_ids(raw: str | None) -> tuple[int | str, ...]:
    values: list[int | str] = []
    for item in _split_csv(raw):
        try:
            values.append(int(item))
        except (TypeError, ValueError):
            logger.warning("SENDER_ROLLOUT | invalid rule_id ignored | value=%s", item)
    return tuple(values)


def _split_csv(raw: str | None) -> Iterable[str]:
    for item in str(raw or "").split(","):
        value = item.strip()
        if value:
            yield value
