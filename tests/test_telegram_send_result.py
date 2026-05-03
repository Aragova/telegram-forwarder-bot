from app.telegram_send_result import telegram_send_failure, telegram_send_result_from_raw, telegram_send_success


def test_raw_object_with_message_id():
    class Raw:
        message_id = 123

    res = telegram_send_result_from_raw(Raw(), method="copy_single")
    assert res.ok is True
    assert res.sent_message_ids == [123]
    assert res.sent_message_id == 123


def test_raw_object_with_id():
    class Raw:
        id = 124

    res = telegram_send_result_from_raw(Raw(), method="copy_single")
    assert res.sent_message_ids == [124]


def test_list_of_objects():
    class Raw:
        def __init__(self, message_id):
            self.message_id = message_id

    res = telegram_send_result_from_raw([Raw(1), Raw(2)], method="reupload_album")
    assert res.ok is True
    assert res.sent_message_ids == [1, 2]


def test_dict_sent_message_ids():
    res = telegram_send_result_from_raw({"ok": True, "sent_message_ids": [10, "11", 0]}, method="reupload_album")
    assert res.ok is True
    assert res.sent_message_ids == [10, 11]


def test_dict_sent_message_id():
    res = telegram_send_result_from_raw({"ok": True, "sent_message_id": "55"}, method="copy_single")
    assert res.sent_message_ids == [55]
    assert res.sent_message_id == 55


def test_fallback_sent_ids():
    res = telegram_send_result_from_raw(None, method="reupload_single", fallback_sent_ids=[77])
    assert res.ok is True
    assert res.sent_message_ids == [77]


def test_invalid_ids_are_failure():
    res = telegram_send_result_from_raw({"ok": True, "sent_message_ids": [0, "0", None]}, method="copy_single")
    assert res.ok is False
    assert res.sent_message_ids == []


def test_failure_helper():
    res = telegram_send_failure(method="copy_single", error_text="bad", retryable=False)
    assert res.ok is False
    assert res.retryable is False
    assert res.error_text == "bad"


def test_success_helper_filters_ids():
    res = telegram_send_success(method="copy_single", sent_message_ids=[1, 0, "2"])
    assert res.ok is True
    assert res.sent_message_ids == [1, 2]
