from app.telegram_send_result import telegram_send_result_from_raw
from app.sender import SenderService
import asyncio


def test_copy_single_send_result_extracts_message_id():
    class Raw:
        message_id = 123

    res = telegram_send_result_from_raw(Raw(), method="copy_single")
    assert res.ok is True
    assert res.sent_message_ids == [123]


def test_reupload_single_send_result_uses_fallback_id():
    res = telegram_send_result_from_raw(None, method="reupload_single", fallback_sent_ids=[222])
    assert res.ok is True
    assert res.sent_message_ids == [222]


def test_reupload_album_send_result_ids_and_verify_candidates():
    raw = {"ok": True, "sent_message_ids": [301, 302, 303]}
    res = telegram_send_result_from_raw(raw, method="reupload_album")
    assert res.ok is True
    assert res.sent_message_ids == [301, 302, 303]
    assert len(res.sent_message_ids) == 3


def test_invalid_raw_result_does_not_pass_accepted_condition():
    raw = {"ok": True, "sent_message_ids": [0]}
    res = telegram_send_result_from_raw(raw, method="copy_single")
    assert res.ok is False
    assert res.sent_message_ids == []


def test_telegram_send_result_ok_false_when_message_id_non_positive():
    class Raw:
        message_id = 0

    res = telegram_send_result_from_raw(Raw(), method="copy_single")
    assert res.ok is False
    assert res.sent_message_ids == []


def test_copy_single_via_bot_keeps_legacy_dict_and_adds_raw_result():
    class Raw:
        message_id = 123

    class Bot:
        async def copy_message(self, **_kwargs):
            return Raw()

    svc = SenderService.__new__(SenderService)
    svc.bot = Bot()

    result = asyncio.run(
        svc._copy_single_via_bot(
            source_channel="@src",
            target_id="@dst",
            message_id=11,
            target_thread_id=None,
        )
    )
    assert "attempted" in result
    assert "sent_ids" in result
    assert "fallback_allowed" in result
    assert "raw_result_type" in result
    assert result["sent_ids"] == [123]
    assert result["raw_result"] is not None
