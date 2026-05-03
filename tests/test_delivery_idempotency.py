from app.delivery_idempotency import build_delivery_idempotency_key, extract_sent_message_ids_from_attempt, normalize_valid_sent_message_ids


def test_key_builder_single():
    assert build_delivery_idempotency_key(operation_kind="single", delivery_id=510789, target_id="-1003812542665") == "delivery:510789:target:-1003812542665:single"


def test_key_builder_album_with_media_group():
    assert build_delivery_idempotency_key(operation_kind="album", rule_id=89, target_id="-1003812542665", media_group_id="14211706065005946") == "rule:89:target:-1003812542665:media_group:14211706065005946:album"


def test_key_builder_album_without_media_group_sorted_sources():
    a = build_delivery_idempotency_key(operation_kind="album", rule_id=89, target_id="-1003812542665", source_message_ids=[3, 1, 2])
    b = build_delivery_idempotency_key(operation_kind="album", rule_id=89, target_id="-1003812542665", source_message_ids=[1, 2, 3])
    assert a == b


def test_extract_sent_ids():
    assert extract_sent_message_ids_from_attempt({"sent_message_ids_json": [1, 2, 3]}) == [1, 2, 3]
    assert extract_sent_message_ids_from_attempt({"sent_message_ids_json": "[1,2,3]"}) == [1, 2, 3]
    assert extract_sent_message_ids_from_attempt({"sent_message_ids_json": None}) == []
    assert extract_sent_message_ids_from_attempt({"sent_message_ids_json": "oops"}) == []


def test_normalize_valid_sent_message_ids_filters_zero_and_invalid():
    assert normalize_valid_sent_message_ids([0, "0", None, "123", 456, -1]) == [123, 456]


def test_extract_sent_message_ids_from_attempt_filters_zero_and_invalid():
    attempt = {"sent_message_ids_json": [0, "0", None, "123", 456, -1]}
    assert extract_sent_message_ids_from_attempt(attempt) == [123, 456]
