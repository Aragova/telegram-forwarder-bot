from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from inspect import isawaitable
from typing import Any

from app.delivery_context import DeliveryContext
from app.transport_policy import TransportRateLimited


class ReactionPostSendStatus(str, Enum):
    APPLIED = "applied"
    PARTIAL = "partial"
    SKIPPED = "skipped"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"


def normalize_reaction_message_ids(
    message_ids: tuple[int, ...] | list[int] | Iterable[int] | int | None,
) -> tuple[int, ...]:
    if message_ids is None:
        return ()
    if isinstance(message_ids, int):
        return (message_ids,)
    return tuple(message_ids)


def normalize_reactions(
    reactions: tuple[str, ...] | list[str] | Iterable[str] | str | None,
) -> tuple[str, ...]:
    if reactions is None:
        return ()
    if isinstance(reactions, str):
        return (reactions,) if reactions else ()
    return tuple(reaction for reaction in reactions if reaction)


@dataclass(frozen=True, slots=True)
class ReactionPostSendInput:
    context: DeliveryContext | None = None
    target_chat_id: int | str | None = None
    target_message_ids: tuple[int, ...] | list[int] | Iterable[int] | int | None = None
    reactions: tuple[str, ...] | list[str] | Iterable[str] | str | None = None
    enabled: bool = True
    idempotency_key: str | None = None
    reaction_label: str | None = None
    continue_on_error: bool = False

    def to_log_context(self) -> dict[str, object]:
        target_message_ids = normalize_reaction_message_ids(self.target_message_ids)
        normalized_reactions = normalize_reactions(self.reactions)
        context: dict[str, object] = {
            "target_chat_id": self.target_chat_id,
            "target_message_ids": target_message_ids,
            "target_message_count": len(target_message_ids),
            "enabled": self.enabled,
            "idempotency_key": self.idempotency_key,
            "reaction_count": len(normalized_reactions),
            "has_reactions": bool(normalized_reactions),
            "has_reaction_label": bool(self.reaction_label),
            "continue_on_error": self.continue_on_error,
        }
        if self.context is not None:
            context["context"] = self.context.to_log_context()
        return context


@dataclass(frozen=True, slots=True)
class ReactionPostSendResult:
    status: ReactionPostSendStatus
    context: DeliveryContext | None
    target_chat_id: int | str | None
    target_message_ids: tuple[int, ...]
    applied_message_ids: tuple[int, ...]
    failed_message_ids: tuple[int, ...]
    reaction_count: int
    idempotency_key: str | None = None
    retry_after: int | float | None = None
    error_type: str | None = None
    error_text: str | None = None
    reason: str | None = None

    @classmethod
    def applied(cls, **kwargs: Any) -> "ReactionPostSendResult":
        return cls(status=ReactionPostSendStatus.APPLIED, **kwargs)

    @classmethod
    def partial(cls, **kwargs: Any) -> "ReactionPostSendResult":
        return cls(status=ReactionPostSendStatus.PARTIAL, **kwargs)

    @classmethod
    def skipped(cls, **kwargs: Any) -> "ReactionPostSendResult":
        return cls(status=ReactionPostSendStatus.SKIPPED, **kwargs)

    @classmethod
    def failed(cls, **kwargs: Any) -> "ReactionPostSendResult":
        return cls(status=ReactionPostSendStatus.FAILED, **kwargs)

    @classmethod
    def rate_limited(cls, **kwargs: Any) -> "ReactionPostSendResult":
        return cls(status=ReactionPostSendStatus.RATE_LIMITED, **kwargs)

    @property
    def ok(self) -> bool:
        return self.status == ReactionPostSendStatus.APPLIED

    @property
    def is_applied(self) -> bool:
        return self.status == ReactionPostSendStatus.APPLIED

    @property
    def is_partial(self) -> bool:
        return self.status == ReactionPostSendStatus.PARTIAL

    @property
    def is_skipped(self) -> bool:
        return self.status == ReactionPostSendStatus.SKIPPED

    @property
    def is_failure(self) -> bool:
        return self.status == ReactionPostSendStatus.FAILED

    @property
    def should_defer(self) -> bool:
        return self.status == ReactionPostSendStatus.RATE_LIMITED

    def to_log_context(self) -> dict[str, object]:
        context: dict[str, object] = {
            "status": self.status.value,
            "target_chat_id": self.target_chat_id,
            "target_message_ids": self.target_message_ids,
            "target_message_count": len(self.target_message_ids),
            "applied_message_ids": self.applied_message_ids,
            "applied_message_count": len(self.applied_message_ids),
            "failed_message_ids": self.failed_message_ids,
            "failed_message_count": len(self.failed_message_ids),
            "reaction_count": self.reaction_count,
            "idempotency_key": self.idempotency_key,
            "retry_after": self.retry_after,
            "error_type": self.error_type,
            "error_text": self.error_text,
            "reason": self.reason,
        }
        if self.context is not None:
            context["context"] = self.context.to_log_context()
        return context

    def log_label(self) -> str:
        return (
            f"reaction_post_send status={self.status.value} target={self.target_chat_id} "
            f"messages={len(self.target_message_ids)} applied={len(self.applied_message_ids)} "
            f"failed={len(self.failed_message_ids)}"
        )


class ReactionPostSendService:
    def __init__(self, *, reaction_sender: Any | None) -> None:
        self.reaction_sender = reaction_sender

    async def run(self, input_data: ReactionPostSendInput) -> ReactionPostSendResult:
        target_message_ids = normalize_reaction_message_ids(input_data.target_message_ids)
        reactions = normalize_reactions(input_data.reactions)
        context = input_data.context.with_operation("reaction_post_send") if input_data.context else None
        common = {
            "context": context,
            "target_chat_id": input_data.target_chat_id,
            "target_message_ids": target_message_ids,
            "applied_message_ids": (),
            "failed_message_ids": (),
            "reaction_count": len(reactions),
            "idempotency_key": input_data.idempotency_key,
        }
        if not input_data.enabled:
            return ReactionPostSendResult.skipped(**common, reason="reactions_disabled")
        if not target_message_ids:
            return ReactionPostSendResult.skipped(**common, reason="missing_target_message_ids")
        if not reactions:
            return ReactionPostSendResult.skipped(**common, reason="missing_reactions")
        if input_data.target_chat_id is None:
            return ReactionPostSendResult.failed(**common, reason="missing_target_chat_id")
        call = self._get_apply_reaction_call()
        if call is None:
            return ReactionPostSendResult.failed(**common, reason="reaction_sender_not_configured")

        applied: list[int] = []
        failed: list[int] = []
        last_error_type: str | None = None
        last_error_text: str | None = None
        for message_id in target_message_ids:
            try:
                result = call(
                    chat_id=input_data.target_chat_id,
                    message_id=message_id,
                    reactions=reactions,
                    context=context,
                )
                if isawaitable(result):
                    result = await result
                if result is False:
                    failed.append(message_id)
                    if not input_data.continue_on_error:
                        return self._failed_result(input_data, context, target_message_ids, reactions, applied, failed)
                    continue
                applied.append(message_id)
            except TransportRateLimited as error:
                failed.append(message_id)
                return ReactionPostSendResult.rate_limited(
                    context=context,
                    target_chat_id=input_data.target_chat_id,
                    target_message_ids=target_message_ids,
                    applied_message_ids=tuple(applied),
                    failed_message_ids=tuple(failed),
                    reaction_count=len(reactions),
                    idempotency_key=input_data.idempotency_key,
                    retry_after=error.retry_after_seconds,
                    error_type=type(error).__name__,
                    error_text=_safe_error_text(error),
                    reason="reaction_rate_limited",
                )
            except Exception as error:
                failed.append(message_id)
                last_error_type = type(error).__name__
                last_error_text = _safe_error_text(error)
                if not input_data.continue_on_error:
                    return self._failed_result(
                        input_data,
                        context,
                        target_message_ids,
                        reactions,
                        applied,
                        failed,
                        error_type=last_error_type,
                        error_text=last_error_text,
                    )

        if failed and applied:
            return ReactionPostSendResult.partial(
                context=context,
                target_chat_id=input_data.target_chat_id,
                target_message_ids=target_message_ids,
                applied_message_ids=tuple(applied),
                failed_message_ids=tuple(failed),
                reaction_count=len(reactions),
                idempotency_key=input_data.idempotency_key,
                error_type=last_error_type,
                error_text=last_error_text,
                reason="reaction_apply_failed",
            )
        if failed:
            return self._failed_result(
                input_data,
                context,
                target_message_ids,
                reactions,
                applied,
                failed,
                error_type=last_error_type,
                error_text=last_error_text,
            )
        return ReactionPostSendResult.applied(
            context=context,
            target_chat_id=input_data.target_chat_id,
            target_message_ids=target_message_ids,
            applied_message_ids=tuple(applied),
            failed_message_ids=(),
            reaction_count=len(reactions),
            idempotency_key=input_data.idempotency_key,
        )

    def _get_apply_reaction_call(self) -> Any | None:
        if self.reaction_sender is None:
            return None
        apply_reaction = getattr(self.reaction_sender, "apply_reaction", None)
        if apply_reaction is not None:
            return apply_reaction
        if callable(self.reaction_sender):
            return self.reaction_sender
        return None

    @staticmethod
    def _failed_result(
        input_data: ReactionPostSendInput,
        context: DeliveryContext | None,
        target_message_ids: tuple[int, ...],
        reactions: tuple[str, ...],
        applied: list[int],
        failed: list[int],
        *,
        error_type: str | None = None,
        error_text: str | None = None,
    ) -> ReactionPostSendResult:
        return ReactionPostSendResult.failed(
            context=context,
            target_chat_id=input_data.target_chat_id,
            target_message_ids=target_message_ids,
            applied_message_ids=tuple(applied),
            failed_message_ids=tuple(failed),
            reaction_count=len(reactions),
            idempotency_key=input_data.idempotency_key,
            error_type=error_type,
            error_text=error_text,
            reason="reaction_apply_failed",
        )


def _safe_error_text(error: Exception, *, max_len: int = 160) -> str:
    text = " ".join(str(error).replace("\n", " ").replace("\r", " ").split())
    if len(text) > max_len:
        return f"{text[: max_len - 3]}..."
    return text
