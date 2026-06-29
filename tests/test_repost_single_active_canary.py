from __future__ import annotations

import asyncio
from types import SimpleNamespace

from app.delivery_finalizer import DeliveryFinalizationResult, DeliveryFinalizationStatus, DeliveryOutcome
from app.repost_single_active_canary import RepostSingleActiveCanaryConfig, RepostSingleActiveCanaryRunner, RepostSingleActiveCanaryStatus
from app.repost_single_rollout_probe import RepostSingleProbeResult, RepostSingleProbeStatus
from app.sender_pipeline_rollout import SenderPipelineName, SenderPipelineRolloutDecision, SenderRolloutAction, SenderRolloutMode


class FakePipeline:
    def __init__(self, result=None, error=None):
        self.calls = []
        self.result = result or DeliveryFinalizationResult.finalized(pipeline_status="sent")
        self.error = error

    async def run(self, input_data):
        self.calls.append(input_data)
        if self.error:
            raise self.error
        return self.result


def run(coro):
    return asyncio.run(coro)


def probe(mode=SenderRolloutMode.ACTIVE, action=SenderRolloutAction.USE_PIPELINE, rule_id=12):
    return RepostSingleProbeResult(
        status=RepostSingleProbeStatus.ACTIVE_NOT_ENABLED,
        decision=SenderPipelineRolloutDecision(
            pipeline_name=SenderPipelineName.REPOST_SINGLE,
            mode=mode,
            action=action,
            rule_id=rule_id,
            should_call_pipeline=action is SenderRolloutAction.USE_PIPELINE,
            should_use_pipeline_result=action is SenderRolloutAction.USE_PIPELINE,
            should_continue_legacy=action is not SenderRolloutAction.USE_PIPELINE,
        ),
        rule_id=rule_id,
        source_id=-100,
        target_id=-200,
        source_message_id=10,
    )


def runner(pipeline):
    return RepostSingleActiveCanaryRunner(pipeline=pipeline, config=RepostSingleActiveCanaryConfig(canary_enabled=True, enabled_rule_ids=(12,)))


def kwargs(**overrides):
    values = dict(probe_result=probe(), rule=SimpleNamespace(id=12), delivery_id=1, message_id=10, source_channel=-100, target_id=-200, target_thread_id=55, post_id=3, idempotency_key="k")
    values.update(overrides)
    return values


def test_eligible_active_canary_calls_pipeline_once_and_returns_handled():
    pipeline = FakePipeline()

    result = run(runner(pipeline).try_run(**kwargs()))

    assert len(pipeline.calls) == 1
    assert result.status is RepostSingleActiveCanaryStatus.HANDLED
    assert result.attempted_pipeline is True
    assert result.should_continue_legacy is False
    assert pipeline.calls[0].source_chat_id == -100
    assert pipeline.calls[0].target_chat_id == -200


def test_pipeline_failure_returns_failed_and_no_legacy_fallback():
    pipeline = FakePipeline(result=DeliveryFinalizationResult.failed(reason="copy_message_failed", pipeline_status="failed"))

    result = run(runner(pipeline).try_run(**kwargs()))

    assert len(pipeline.calls) == 1
    assert result.status is RepostSingleActiveCanaryStatus.FAILED
    assert result.attempted_pipeline is True
    assert result.should_continue_legacy is False


def test_pipeline_exception_returns_failed_and_no_legacy_fallback():
    pipeline = FakePipeline(error=RuntimeError("boom"))

    result = run(runner(pipeline).try_run(**kwargs()))

    assert len(pipeline.calls) == 1
    assert result.status is RepostSingleActiveCanaryStatus.FAILED
    assert result.reason == "pipeline_exception:RuntimeError"
    assert result.should_continue_legacy is False


def test_dry_run_shadow_disabled_do_not_call_pipeline():
    for mode, action in (
        (SenderRolloutMode.DRY_RUN, SenderRolloutAction.DRY_RUN_PIPELINE),
        (SenderRolloutMode.SHADOW, SenderRolloutAction.SHADOW_PIPELINE),
        (SenderRolloutMode.DISABLED, SenderRolloutAction.USE_LEGACY),
    ):
        pipeline = FakePipeline()
        result = run(runner(pipeline).try_run(**kwargs(probe_result=probe(mode=mode, action=action))))
        assert pipeline.calls == []
        assert result.status is RepostSingleActiveCanaryStatus.DISABLED
        assert result.should_continue_legacy is True


def test_missing_required_fields_do_not_call_pipeline():
    pipeline = FakePipeline()

    result = run(runner(pipeline).try_run(**kwargs(message_id=None)))

    assert pipeline.calls == []
    assert result.status is RepostSingleActiveCanaryStatus.NOT_READY
    assert result.reason == "missing_source_message_id"


def test_unsupported_feature_does_not_call_pipeline():
    pipeline = FakePipeline()

    result = run(runner(pipeline).try_run(**kwargs(unsupported_features=("reactions",))))

    assert pipeline.calls == []
    assert result.status is RepostSingleActiveCanaryStatus.NOT_READY
    assert result.reason == "unsupported_feature:reactions"
