from __future__ import annotations

import asyncio
import logging

from .delivery_idempotency import normalize_valid_sent_message_ids

logger = logging.getLogger("forwarder")


class SenderPostSendHelpers:
    def __init__(self, owner):
        self.owner = owner

    def extract_sent_message_id(self, sent_msg) -> int | None:
        ids = self.extract_sent_message_ids(sent_msg)
        return ids[0] if ids else None

    def extract_sent_message_ids(self, sent_result) -> list[int]:
        def _extract_one(item) -> list[int]:
            if item is None:
                return []
            if isinstance(item, (list, tuple)):
                nested: list[int] = []
                for nested_item in item:
                    nested.extend(_extract_one(nested_item))
                return nested
            if isinstance(item, dict):
                values = []
                for key in ("message_id", "id"):
                    val = item.get(key)
                    if val is not None:
                        try:
                            values.append(int(val))
                        except Exception:
                            pass
                if values:
                    return values
                for nested_key in ("message", "result", "data"):
                    if nested_key in item:
                        nested_values = _extract_one(item.get(nested_key))
                        if nested_values:
                            return nested_values
                return values
            for attr in ("message_id", "id"):
                try:
                    val = getattr(item, attr, None)
                    if val is not None:
                        return [int(val)]
                except Exception:
                    continue
            for key in ("message", "result", "data"):
                nested_obj = getattr(item, key, None)
                if nested_obj is not None:
                    nested_values = _extract_one(nested_obj)
                    if nested_values:
                        return nested_values
            return []

        if sent_result is None:
            return []
        raw_items = list(sent_result) if isinstance(sent_result, (list, tuple)) else [sent_result]
        ids: list[int] = []
        for item in raw_items:
            ids.extend(_extract_one(item))
        return [x for x in ids if isinstance(x, int)]

    async def validate_reaction_target_message(
        self,
        *,
        rule_id: int | None,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        sent_message_id: int | None,
        delivery_id: int | None = None,
        max_age_seconds: int = 300,
    ) -> int | None:
        from .reaction_delivery import ReactionDelivery

        return await ReactionDelivery(self.owner)._validate_reaction_target_message(
            rule_id=rule_id,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            sent_message_id=sent_message_id,
            delivery_id=delivery_id,
            max_age_seconds=max_age_seconds,
        )

    async def validate_sent_message_ids_for_delivery(
        self,
        *,
        rule_id: int | None,
        delivery_id: int | None,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        candidate_sent_message_ids: list[int],
        method: str,
        max_age_seconds: int = 300,
    ) -> list[int]:
        normalized_candidates: list[int] = []
        for value in candidate_sent_message_ids or []:
            try:
                normalized_candidates.append(int(value))
            except Exception:
                continue

        logger.info(
            "DELIVERY_SENT_MESSAGE_IDS_VALIDATE_START | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | candidate_sent_message_ids=%s",
            rule_id,
            delivery_id,
            method,
            target_id,
            normalized_candidates,
        )

        valid_ids: list[int] = []
        for candidate_id in normalized_candidates:
            try:
                validated = await self.validate_reaction_target_message(
                    rule_id=rule_id,
                    source_channel=str(source_channel or ""),
                    target_id=str(target_id),
                    source_message_ids=source_message_ids or [],
                    sent_message_id=candidate_id,
                    delivery_id=delivery_id,
                    max_age_seconds=max_age_seconds,
                )
            except Exception as exc:
                logger.warning(
                    "DELIVERY_SENT_MESSAGE_IDS_VALIDATE_ITEM_FAILED | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | sent_message_id=%s | error=%s",
                    rule_id,
                    delivery_id,
                    method,
                    target_id,
                    candidate_id,
                    exc,
                )
                continue
            if validated:
                valid_ids.append(int(validated))

        if valid_ids:
            logger.info(
                "DELIVERY_SENT_MESSAGE_IDS_VALIDATE_OK | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | valid_sent_message_ids=%s",
                rule_id,
                delivery_id,
                method,
                target_id,
                valid_ids,
            )
            return valid_ids

        reason = "no_candidate_ids" if not normalized_candidates else "all_candidates_rejected"
        logger.warning(
            "DELIVERY_SENT_MESSAGE_IDS_VALIDATE_EMPTY | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | candidate_sent_message_ids=%s | reason=%s",
            rule_id,
            delivery_id,
            method,
            target_id,
            normalized_candidates,
            reason,
        )
        return []

    async def confirm_target_delivery_message_ids(
        self,
        *,
        rule_id: int | None,
        delivery_id: int | None,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        candidate_sent_message_ids: list[int],
        method: str,
        max_age_seconds: int = 300,
    ) -> list[int]:
        normalized_candidates: list[int] = []
        for value in candidate_sent_message_ids or []:
            try:
                normalized_candidates.append(int(value))
            except Exception:
                continue

        logger.info(
            "DELIVERY_TARGET_CONFIRM_START | rule_id=%s | delivery_id=%s | method=%s | source_channel=%s | target_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s",
            rule_id,
            delivery_id,
            method,
            source_channel,
            target_id,
            source_message_ids,
            normalized_candidates,
        )

        if not hasattr(self.owner.telethon, "get_messages"):
            logger.warning(
                "DELIVERY_TARGET_CONFIRM_SKIPPED_NO_GET_MESSAGES | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | candidate_sent_message_ids=%s",
                rule_id,
                delivery_id,
                method,
                target_id,
                normalized_candidates,
            )
            return normalized_candidates

        if not normalized_candidates:
            logger.warning(
                "DELIVERY_TARGET_CONFIRM_FAILED | rule_id=%s | delivery_id=%s | method=%s | source_channel=%s | target_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s | reason=%s",
                rule_id, delivery_id, method, source_channel, target_id, source_message_ids, normalized_candidates, "no_candidate_ids"
            )
            return []

        valid_ids = await self.validate_sent_message_ids_for_delivery(
            rule_id=rule_id,
            delivery_id=delivery_id,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            candidate_sent_message_ids=normalized_candidates,
            method=method,
            max_age_seconds=max_age_seconds,
        )

        if not valid_ids:
            logger.warning(
                "DELIVERY_TARGET_CONFIRM_FAILED | rule_id=%s | delivery_id=%s | method=%s | source_channel=%s | target_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s | reason=%s",
                rule_id, delivery_id, method, source_channel, target_id, source_message_ids, normalized_candidates, "all_candidates_rejected"
            )
            return []

        logger.info(
            "DELIVERY_TARGET_CONFIRM_OK | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | valid_sent_message_ids=%s",
            rule_id,
            delivery_id,
            method,
            target_id,
            valid_ids,
        )
        return valid_ids

    async def confirm_target_delivery_message_ids_with_retry(
        self,
        **kwargs,
    ) -> list[int]:
        attempts = (0.0, 0.7, 1.5)
        last_reason = "target_message_not_found_after_send"
        for attempt_no, delay_seconds in enumerate(attempts, start=1):
            if delay_seconds > 0:
                logger.info(
                    "DELIVERY_TARGET_CONFIRM_RETRY | rule_id=%s | delivery_id=%s | attempt=%s | delay=%s | candidate_sent_message_ids=%s | reason=%s",
                    kwargs.get("rule_id"), kwargs.get("delivery_id"), attempt_no, delay_seconds, kwargs.get("candidate_sent_message_ids"), last_reason
                )
                await asyncio.sleep(delay_seconds)
            valid_ids = await self.confirm_target_delivery_message_ids(**kwargs)
            if valid_ids:
                return valid_ids
        return []

    async def run_post_send_step_safe(
        self,
        *,
        step_name: str,
        rule_id: int | None,
        delivery_id: int | None,
        idempotency_key: str | None = None,
        accepted_sent_message_ids: list[int] | None = None,
        coro_factory=None,
    ) -> dict:
        try:
            result = await coro_factory()
            return {"ok": True, "result": result}
        except Exception as exc:
            logger.warning(
                "POST_SEND_STEP_FAILED_NON_FATAL | step_name=%s | rule_id=%s | delivery_id=%s | idempotency_key=%s | accepted_sent_message_ids=%s | error=%s",
                step_name,
                rule_id,
                delivery_id,
                idempotency_key,
                accepted_sent_message_ids or [],
                exc,
            )
            return {"ok": False, "error": str(exc)}
