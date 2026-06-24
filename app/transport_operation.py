from __future__ import annotations

import re
from enum import StrEnum


class TransportOperationKind(StrEnum):
    SAFE_READ = "safe_read"
    DOWNLOAD = "download"
    VERIFY = "verify"
    NON_IDEMPOTENT_WRITE = "non_idempotent_write"
    REACTION = "reaction"
    UNKNOWN = "unknown"


_NON_IDEMPOTENT_WRITE_OPERATIONS = frozenset(
    {
        "copy_message",
        "copy_messages",
        "forward_message",
        "forward_messages",
        "send_message",
        "send_photo",
        "send_video",
        "send_document",
        "send_audio",
        "send_animation",
        "send_voice",
        "send_video_note",
        "send_media_group",
        "send_file",
        "send_album",
    }
)

_SAFE_READ_OPERATIONS = frozenset(
    {
        "get_messages",
        "get_message",
        "get_chat",
        "get_entity",
        "get_input_entity",
        "iter_messages",
    }
)

_DOWNLOAD_OPERATIONS = frozenset(
    {
        "download_media",
        "download_file",
    }
)

_REACTION_OPERATIONS = frozenset(
    {
        "send_reaction",
        "set_message_reaction",
        "set_message_reactions",
    }
)

_OPERATION_KIND_VALUES = {kind.value for kind in TransportOperationKind}


def normalize_transport_operation_name(op_name: str | None) -> str:
    value = str(op_name or "").strip()
    if not value:
        return ""

    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"[\s\-]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_").lower()


def _coerce_operation_kind(value: TransportOperationKind | str | None) -> TransportOperationKind | None:
    if value is None:
        return None
    if isinstance(value, TransportOperationKind):
        return value

    normalized = normalize_transport_operation_name(str(value))
    if normalized in _OPERATION_KIND_VALUES:
        return TransportOperationKind(normalized)
    return None


def classify_transport_operation(
    backend: str,
    op_name: str,
    *,
    explicit_kind: TransportOperationKind | str | None = None,
) -> TransportOperationKind:
    override = _coerce_operation_kind(explicit_kind)
    if override is not None:
        return override

    normalized_op_name = normalize_transport_operation_name(op_name)
    if normalized_op_name in _NON_IDEMPOTENT_WRITE_OPERATIONS:
        return TransportOperationKind.NON_IDEMPOTENT_WRITE
    if normalized_op_name in _SAFE_READ_OPERATIONS:
        return TransportOperationKind.SAFE_READ
    if normalized_op_name in _DOWNLOAD_OPERATIONS:
        return TransportOperationKind.DOWNLOAD
    if normalized_op_name in _REACTION_OPERATIONS:
        return TransportOperationKind.REACTION
    return TransportOperationKind.UNKNOWN
