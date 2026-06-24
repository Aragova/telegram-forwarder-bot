from __future__ import annotations

import asyncio
from dataclasses import fields

from app.delivery_context import DeliveryContext
from app.target_verifier import TargetVerificationResult, TargetVerificationStatus, TargetVerifier
from app.telegram_send_result import telegram_send_success
from app.transport_policy import TransportRateLimited


class FakeMessage:
    def __init__(self, *, id=None, message_id=None):
        self.id = id
        self.message_id = message_id


class FakeTelethon:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.calls = []

    async def get_messages(self, target_id, *, ids):
        self.calls.append({"target_id": target_id, "ids": ids})
        if self.error is not None:
            raise self.error
        return self.result


def test_result_verified_construction():
    result = TargetVerificationResult.verified(target_id=-100, message_ids=[101], found_message_ids=[101])

    assert result.status == TargetVerificationStatus.VERIFIED
    assert result.ok is True
    assert result.is_failure is False
    assert result.should_defer is False


def test_result_not_found_construction():
    result = TargetVerificationResult.not_found(
        target_id=-100,
        message_ids=[101, 102],
        found_message_ids=[101],
        missing_message_ids=[102],
    )

    assert result.status == TargetVerificationStatus.NOT_FOUND
    assert result.ok is False
    assert result.is_failure is True
    assert result.missing_message_ids == (102,)


def test_result_rate_limited_construction():
    result = TargetVerificationResult.rate_limited(target_id=-100, message_ids=[101], retry_after=60)

    assert result.status == TargetVerificationStatus.RATE_LIMITED
    assert result.should_defer is True
    assert result.retry_after == 60


def test_result_failed_from_exception_does_not_store_raw_exception():
    error = RuntimeError("boom")
    result = TargetVerificationResult.failed(target_id=-100, message_ids=[101], error=error)

    assert result.status == TargetVerificationStatus.FAILED
    assert result.error_type == "RuntimeError"
    assert "boom" in result.error_text
    assert all(getattr(result, field.name) is not error for field in fields(TargetVerificationResult))


def test_verify_message_exists_verified():
    async def scenario():
        client = FakeTelethon(result=FakeMessage(id=101))
        result = await TargetVerifier(telethon_client=client).verify_message_exists(target_id=-100, message_id=101)

        assert result.status == TargetVerificationStatus.VERIFIED
        assert result.found_message_ids == (101,)
        assert result.missing_message_ids == ()
        assert client.calls == [{"target_id": -100, "ids": 101}]

    asyncio.run(scenario())


def test_verify_message_exists_not_found():
    async def scenario():
        result = await TargetVerifier(telethon_client=FakeTelethon(result=None)).verify_message_exists(
            target_id=-100,
            message_id=101,
        )

        assert result.status == TargetVerificationStatus.NOT_FOUND
        assert result.missing_message_ids == (101,)

    asyncio.run(scenario())


def test_verify_message_ids_normalizes_ids_and_detects_partial_missing():
    async def scenario():
        client = FakeTelethon(result=[FakeMessage(id=101), FakeMessage(id=103)])
        result = await TargetVerifier(telethon_client=client).verify_message_ids(
            target_id=-100,
            message_ids=[101, 102, 103],
        )

        assert result.message_ids == (101, 102, 103)
        assert result.found_message_ids == (101, 103)
        assert result.missing_message_ids == (102,)
        assert result.status == TargetVerificationStatus.NOT_FOUND
        assert client.calls == [{"target_id": -100, "ids": [101, 102, 103]}]

    asyncio.run(scenario())


def test_verify_send_result_uses_telegram_send_result_sent_ids_without_storing_raw_result():
    async def scenario():
        send_result = telegram_send_success(method="copy_message", sent_message_ids=[101, 102])
        result = await TargetVerifier(telethon_client=FakeTelethon(result=[{"id": 101}, {"message_id": 102}])).verify_send_result(
            target_id=-100,
            send_result=send_result,
        )

        assert result.status == TargetVerificationStatus.VERIFIED
        assert result.message_ids == (101, 102)
        assert all(getattr(result, field.name) is not send_result for field in fields(TargetVerificationResult))

    asyncio.run(scenario())


def test_missing_ids_are_skipped():
    async def scenario():
        verifier = TargetVerifier(telethon_client=FakeTelethon(result=FakeMessage(id=101)))
        empty_send_result = telegram_send_success(method="copy_message", sent_message_ids=[])

        assert (await verifier.verify_message_exists(target_id=-100, message_id=None)).status == TargetVerificationStatus.SKIPPED
        assert (await verifier.verify_message_ids(target_id=-100, message_ids=None)).status == TargetVerificationStatus.SKIPPED
        assert (await verifier.verify_message_ids(target_id=-100, message_ids=[])).status == TargetVerificationStatus.SKIPPED
        assert (await verifier.verify_send_result(target_id=-100, send_result=empty_send_result)).status == TargetVerificationStatus.SKIPPED

    asyncio.run(scenario())


def test_transport_rate_limited_converts_to_rate_limited_result_without_retry():
    async def scenario():
        error = TransportRateLimited(retry_after_seconds=60, backend="telethon", op_name="get_messages", key="sender.telethon.get_messages")
        client = FakeTelethon(error=error)
        result = await TargetVerifier(telethon_client=client).verify_message_ids(target_id=-100, message_ids=[101])

        assert result.status == TargetVerificationStatus.RATE_LIMITED
        assert result.should_defer is True
        assert result.retry_after == 60
        assert len(client.calls) == 1

    asyncio.run(scenario())


def test_generic_exception_converts_to_failed_result():
    async def scenario():
        result = await TargetVerifier(telethon_client=FakeTelethon(error=RuntimeError("boom"))).verify_message_ids(
            target_id=-100,
            message_ids=[101],
        )

        assert result.status == TargetVerificationStatus.FAILED
        assert result.error_type == "RuntimeError"

    asyncio.run(scenario())


def test_target_verifier_does_not_access_raw():
    class FakeProxyTelethon:
        @property
        def raw(self):
            raise AssertionError("raw accessed")

        async def get_messages(self, target_id, *, ids):
            return FakeMessage(id=101)

    async def scenario():
        result = await TargetVerifier(telethon_client=FakeProxyTelethon()).verify_message_exists(
            target_id=-100,
            message_id=101,
        )

        assert result.status == TargetVerificationStatus.VERIFIED

    asyncio.run(scenario())


def test_safe_log_context_and_label_do_not_include_sensitive_values():
    context = DeliveryContext.from_job_payload(
        {
            "delivery_id": 10,
            "rule_id": 5,
            "target_id": -100,
            "caption": "PRIVATE CAPTION",
            "content_json": "PRIVATE TEXT",
            "bot_token": "123:SECRET_TOKEN",
        }
    )
    result = TargetVerificationResult.not_found(
        target_id=-100,
        message_ids=[101, 102],
        found_message_ids=[101],
        missing_message_ids=[102],
        context=context,
    )

    log_context = result.to_log_context()
    label = result.log_label()
    combined = f"{log_context} {label}"

    assert log_context["status"] == "not_found"
    assert log_context["target_id"] == -100
    assert log_context["message_count"] == 2
    assert log_context["found_count"] == 1
    assert log_context["missing_count"] == 1
    assert "context" in log_context
    assert "status=not_found" in label
    assert "target=-100" in label
    assert "found=1" in label
    assert "missing=1" in label
    assert "PRIVATE CAPTION" not in combined
    assert "PRIVATE TEXT" not in combined
    assert "123:SECRET_TOKEN" not in combined
