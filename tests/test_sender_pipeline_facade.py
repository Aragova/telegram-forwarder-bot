import asyncio

import pytest

from app.sender_pipeline_facade import (
    SenderPipelineFacade,
    SenderPipelineFacadeResult,
    SenderPipelineFeatureFlags,
)


class FakePipeline:
    def __init__(self):
        self.calls = []

    async def run(self, input_data):
        self.calls.append(input_data)
        return "ok"


class FailingPipeline:
    async def run(self, input_data):
        raise RuntimeError("SECRET_TOKEN boom")


class NoRunPipeline:
    pass


PIPELINE_CASES = [
    ("enable_repost_single_pipeline", "repost_single_pipeline", "try_handle_repost_single"),
    ("enable_repost_album_pipeline", "repost_album_pipeline", "try_handle_repost_album"),
    ("enable_video_send_pipeline", "video_send_pipeline", "try_handle_video_send"),
    (
        "enable_legacy_video_delivery_pipeline",
        "legacy_video_delivery_pipeline",
        "try_handle_legacy_video_delivery",
    ),
    ("enable_repost_campaign_pipeline", "repost_campaign_pipeline", "try_handle_repost_campaign"),
    ("enable_reaction_post_send_service", "reaction_post_send_service", "try_handle_reactions"),
]


def test_default_flags_are_disabled():
    flags = SenderPipelineFeatureFlags()

    assert flags.enable_repost_single_pipeline is False
    assert flags.enable_repost_album_pipeline is False
    assert flags.enable_video_send_pipeline is False
    assert flags.enable_legacy_video_delivery_pipeline is False
    assert flags.enable_repost_campaign_pipeline is False
    assert flags.enable_reaction_post_send_service is False


def test_facade_stores_injected_dependencies():
    fake = object()
    facade = SenderPipelineFacade(
        repost_single_pipeline=fake,
        repost_album_pipeline=fake,
        video_send_pipeline=fake,
        legacy_video_delivery_pipeline=fake,
        repost_campaign_pipeline=fake,
        reaction_post_send_service=fake,
    )

    assert facade.repost_single_pipeline is fake
    assert facade.repost_album_pipeline is fake
    assert facade.video_send_pipeline is fake
    assert facade.legacy_video_delivery_pipeline is fake
    assert facade.repost_campaign_pipeline is fake
    assert facade.reaction_post_send_service is fake


@pytest.mark.parametrize(("flag_name", "dependency_name", "method_name"), PIPELINE_CASES)
def test_disabled_methods_return_not_handled(flag_name, dependency_name, method_name):
    pipeline = FakePipeline()
    facade = SenderPipelineFacade(**{dependency_name: pipeline})

    result = asyncio.run(getattr(facade, method_name)({"private_payload": "SECRET_TOKEN"}))

    assert result.handled is False
    assert result.pipeline_name == dependency_name
    assert result.reason == "pipeline_disabled"
    assert pipeline.calls == []


@pytest.mark.parametrize(("flag_name", "dependency_name", "method_name"), PIPELINE_CASES)
def test_enabled_but_dependency_missing_returns_not_configured(flag_name, dependency_name, method_name):
    flags = SenderPipelineFeatureFlags(**{flag_name: True})
    facade = SenderPipelineFacade(flags=flags)

    result = asyncio.run(getattr(facade, method_name)(object()))

    assert result.handled is False
    assert result.pipeline_name == dependency_name
    assert result.reason == "pipeline_not_configured"


@pytest.mark.parametrize(("flag_name", "dependency_name", "method_name"), PIPELINE_CASES)
def test_enabled_but_dependency_has_no_run_returns_not_configured(flag_name, dependency_name, method_name):
    flags = SenderPipelineFeatureFlags(**{flag_name: True})
    facade = SenderPipelineFacade(flags=flags, **{dependency_name: NoRunPipeline()})

    result = asyncio.run(getattr(facade, method_name)(object()))

    assert result.handled is False
    assert result.pipeline_name == dependency_name
    assert result.reason == "pipeline_not_configured"


@pytest.mark.parametrize(("flag_name", "dependency_name", "method_name"), PIPELINE_CASES)
def test_enabled_dependency_runs(flag_name, dependency_name, method_name):
    flags = SenderPipelineFeatureFlags(**{flag_name: True})
    pipeline = FakePipeline()
    facade = SenderPipelineFacade(flags=flags, **{dependency_name: pipeline})
    input_data = object()

    result = asyncio.run(getattr(facade, method_name)(input_data))

    assert result.handled is True
    assert result.pipeline_name == dependency_name
    assert result.result == "ok"
    assert pipeline.calls == [input_data]


def test_dependency_exception_is_safe():
    flags = SenderPipelineFeatureFlags(enable_repost_single_pipeline=True)
    facade = SenderPipelineFacade(flags=flags, repost_single_pipeline=FailingPipeline())

    result = asyncio.run(facade.try_handle_repost_single({"token": "SECRET_TOKEN"}))

    assert result.handled is False
    assert result.reason == "pipeline_failed"
    assert result.result is None
    assert result.error_type == "RuntimeError"
    assert "SECRET_TOKEN" not in str(result.to_log_context())
    assert "SECRET_TOKEN" not in result.log_label()


def test_result_safe_logs_do_not_include_raw_result_or_payload():
    result = SenderPipelineFacadeResult(
        handled=False,
        pipeline_name="repost_single_pipeline",
        result={"raw_input_data": "SECRET_TOKEN private payload"},
        reason="pipeline_failed",
        error_type="RuntimeError",
        error_text="[redacted]",
    )

    log_context = result.to_log_context()
    log_label = result.log_label()

    assert log_context == {
        "handled": False,
        "pipeline_name": "repost_single_pipeline",
        "reason": "pipeline_failed",
        "error_type": "RuntimeError",
        "error_text": "[redacted]",
    }
    assert "raw_input_data" not in str(log_context)
    assert "SECRET_TOKEN" not in str(log_context)
    assert "private payload" not in log_label
    assert "handled=False" in log_label
    assert "pipeline=repost_single_pipeline" in log_label
    assert "reason=pipeline_failed" in log_label
    assert "error_type=RuntimeError" in log_label
