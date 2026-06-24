from __future__ import annotations

from app.transport_operation import TransportOperationKind, classify_transport_operation


def test_classifier_marks_non_idempotent_write_operations():
    for op_name in ("copy_message", "send_message", "send_media_group", "send_file"):
        assert classify_transport_operation("bot", op_name) == TransportOperationKind.NON_IDEMPOTENT_WRITE


def test_classifier_marks_safe_read_operations():
    assert classify_transport_operation("telethon", "get_messages") == TransportOperationKind.SAFE_READ
    assert classify_transport_operation("bot", "get_chat") == TransportOperationKind.SAFE_READ


def test_classifier_marks_download_operations():
    assert classify_transport_operation("telethon", "download_media") == TransportOperationKind.DOWNLOAD
    assert classify_transport_operation("telethon", "download_file") == TransportOperationKind.DOWNLOAD


def test_classifier_marks_reaction_operations():
    assert classify_transport_operation("bot", "send_reaction") == TransportOperationKind.REACTION
    assert classify_transport_operation("bot", "set_message_reaction") == TransportOperationKind.REACTION


def test_classifier_marks_unknown_operations_as_unknown():
    assert classify_transport_operation("bot", "unknown_method_name") == TransportOperationKind.UNKNOWN


def test_classifier_supports_verify_override_for_read_operations():
    assert classify_transport_operation("telethon", "get_messages") == TransportOperationKind.SAFE_READ
    assert (
        classify_transport_operation(
            "telethon",
            "get_messages",
            explicit_kind=TransportOperationKind.VERIFY,
        )
        == TransportOperationKind.VERIFY
    )


def test_classifier_normalizes_common_name_variants():
    assert classify_transport_operation("bot", " send_message ") == TransportOperationKind.NON_IDEMPOTENT_WRITE
    assert classify_transport_operation("bot", "SEND_MESSAGE") == TransportOperationKind.NON_IDEMPOTENT_WRITE
    assert classify_transport_operation("bot", "sendMessage") == TransportOperationKind.NON_IDEMPOTENT_WRITE
