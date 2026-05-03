from app.delivery_result import normalize_delivery_result


def test_normalize_bool_true():
    assert normalize_delivery_result(True).ok is True


def test_normalize_bool_false():
    assert normalize_delivery_result(False).ok is False


def test_normalize_dict_ok():
    assert normalize_delivery_result({"ok": True}).ok is True


def test_normalize_dict_fallback():
    res = normalize_delivery_result({"ok": False, "fallback_to_legacy": True})
    assert res.fallback_to_legacy is True


def test_normalize_retryable_and_error_text():
    res = normalize_delivery_result({"ok": False, "retryable": False, "error_text": "bad"})
    assert res.retryable is False
    assert res.error_text == "bad"


def test_normalize_sent_message_ids():
    res = normalize_delivery_result({"ok": True, "sent_message_ids": [1, "2", 0, None]})
    assert res.sent_message_ids == [1, 2]
    assert res.sent_message_id == 1


def test_normalize_single_sent_message_id():
    res = normalize_delivery_result({"ok": True, "sent_message_id": "55"})
    assert res.sent_message_ids == [55]
    assert res.sent_message_id == 55


def test_normalize_invalid_ids():
    res = normalize_delivery_result({"ok": True, "sent_message_ids": [0, "0", None]})
    assert res.sent_message_ids == []
    assert res.sent_message_id is None


def test_normalize_unknown_fields_to_extra():
    res = normalize_delivery_result({"ok": True, "custom": "x"})
    assert res.extra["custom"] == "x"
