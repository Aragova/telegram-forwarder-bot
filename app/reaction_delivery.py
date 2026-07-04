from __future__ import annotations

import logging
import random
import time
from typing import Any

from telethon import functions, types

from .reaction_runtime_resolver import ReactionRuntimeResolver
from .runtime_utils import run_db
from .sender_primitives import (
    MAX_NORMAL_REACTION_ATTEMPTS,
    NORMAL_REACTION_POOL,
    _normalize_reaction_emoji,
)
from .telegram_client import ReactionClientInfo

logger = logging.getLogger("forwarder")


class ReactionDelivery:
    def __init__(self, owner):
        self.owner = owner

    # ReactionRuntimeResolver emits REACTION_ACCOUNT_SELECTION; tenant executor emits TENANT_REACTION_DONE.

    async def _validate_reaction_target_message(self, *, rule_id: int | None, source_channel: str, target_id: str, source_message_ids: list[int], sent_message_id: int | None, delivery_id: int | None = None, max_age_seconds: int = 300) -> int | None:
        owner = self.owner
        logger.info("REACTION_TARGET_VALIDATE_START | rule_id=%s | delivery_id=%s | source_channel=%s | target_id=%s | sent_message_id=%s | source_message_ids=%s", rule_id, delivery_id, source_channel, target_id, sent_message_id, source_message_ids)
        if sent_message_id is None or int(sent_message_id) <= 0:
            logger.warning("REACTION_SKIPPED_INVALID_TARGET_MESSAGE | rule_id=%s | delivery_id=%s | source_channel=%s | target_id=%s | sent_message_id=%s | source_message_ids=%s", rule_id, delivery_id, source_channel, target_id, sent_message_id, source_message_ids)
            return None
        entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
        msg = await owner.telethon.get_messages(entity, ids=int(sent_message_id))
        if not msg:
            logger.warning("REACTION_SKIPPED_TARGET_MESSAGE_NOT_FOUND | rule_id=%s | delivery_id=%s | target_id=%s | sent_message_id=%s", rule_id, delivery_id, target_id, sent_message_id)
            return None
        now_ts = int(time.time())
        msg_ts = int(getattr(msg, "date").timestamp()) if getattr(msg, "date", None) else 0
        age_seconds = now_ts - msg_ts if msg_ts else 10**9
        if age_seconds > int(max_age_seconds):
            logger.warning("REACTION_BLOCKED_STALE_SENT_MESSAGE_ID | rule_id=%s | delivery_id=%s | source_channel=%s | target_id=%s | sent_message_id=%s | message_date=%s | age_seconds=%s | max_age_seconds=%s | source_message_ids=%s", rule_id, delivery_id, source_channel, target_id, sent_message_id, getattr(msg, "date", None), age_seconds, max_age_seconds, source_message_ids)
            return None
        if str(source_channel) == str(target_id) and int(sent_message_id) in {int(x) for x in (source_message_ids or [])}:
            logger.warning("REACTION_BLOCKED_SOURCE_MESSAGE_ID | rule_id=%s | delivery_id=%s | source_channel=%s | target_id=%s | sent_message_id=%s | source_message_ids=%s", rule_id, delivery_id, source_channel, target_id, sent_message_id, source_message_ids)
            return None
        logger.info("REACTION_TARGET_VALIDATE_OK | rule_id=%s | delivery_id=%s | target_id=%s | sent_message_id=%s | message_date=%s | age_seconds=%s", rule_id, delivery_id, target_id, sent_message_id, getattr(msg, "date", None), age_seconds)
        return int(sent_message_id)


    async def _try_add_normal_reaction(self, client, entity, sent_message_id, session_name: str, rule_id: int | None = None) -> bool:
        owner = self.owner
        emojis_to_try = NORMAL_REACTION_POOL[:]
        random.shuffle(emojis_to_try)
        emojis_to_try = emojis_to_try[:MAX_NORMAL_REACTION_ATTEMPTS]

        last_error = None

        for emoji in emojis_to_try:
            try:
                await client(
                    functions.messages.SendReactionRequest(
                        peer=entity,
                        msg_id=sent_message_id,
                        reaction=[types.ReactionEmoji(emoticon=emoji)],
                        big=False,
                        add_to_recent=False,
                    )
                )

                confirmed = await self._confirm_reaction(client, entity, sent_message_id, emoji)
                if confirmed:
                    logger.info("REACTION_VISIBLE_CONFIRMED | rule_id=%s | target_id=%s | message_id=%s | session=%s | reaction=%s", rule_id, entity, sent_message_id, session_name, emoji)
                    return True
                logger.warning("REACTION_NOT_VISIBLE_AFTER_SEND | rule_id=%s | target_id=%s | message_id=%s | session=%s | reaction=%s", rule_id, entity, sent_message_id, session_name, emoji)

            except Exception as exc:
                last_error = exc
                exc_text = str(exc).lower()
                if "floodwait" in exc.__class__.__name__.lower() or "flood wait" in exc_text:
                    logger.warning(
                        "NORMAL_REACTION_STOP_ON_FLOOD_WAIT | session=%s | message_id=%s | target_id=%s | error=%s",
                        session_name,
                        sent_message_id,
                        entity,
                        exc,
                    )
                    break
                logger.warning(
                    "Обычный реактор %s не смог поставить реакцию %s на сообщение %s в %s: %s",
                    session_name,
                    emoji,
                    sent_message_id,
                    entity,
                    exc,
                )

        logger.warning(
            "Обычный реактор %s не смог поставить ни одну реакцию на сообщение %s в %s. Последняя ошибка: %s",
            session_name,
            sent_message_id,
            entity,
            last_error,
        )
        return False

    async def _try_add_premium_reactions(self, client, entity, sent_message_id, session_name: str, fixed_reactions: list[str], rule_id: int | None = None) -> bool:
        owner = self.owner
        cleaned = []
        for emoji in fixed_reactions:
            emoji = (emoji or "").strip()
            if emoji and emoji not in cleaned:
                cleaned.append(emoji)

        if not cleaned:
            logger.warning(
                "Premium-реактор %s не имеет закреплённого набора реакций",
                session_name,
            )
            return False

        # Пробуем сперва полный набор, потом 2, потом 1.
        # Fallback на следующий вариант только при исключении RPC.
        variants = []
        if len(cleaned) >= 3:
            variants.append(cleaned[:3])
        if len(cleaned) >= 2:
            variants.append(cleaned[:2])
        variants.append(cleaned[:1])

        last_error = None

        for variant in variants:
            try:
                await client(
                    functions.messages.SendReactionRequest(
                        peer=entity,
                        msg_id=sent_message_id,
                        reaction=[types.ReactionEmoji(emoticon=emoji) for emoji in variant],
                        big=False,
                        add_to_recent=False,
                    )
                )
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Premium-реактор %s не смог поставить реакции %s на сообщение %s в %s: %s",
                    session_name,
                    variant,
                    sent_message_id,
                    entity,
                    exc,
                )
                continue

            confirmed, visible_reactions = await self._confirm_reaction_set(client, entity, sent_message_id, variant)
            if confirmed:
                logger.info(
                    "PREMIUM_REACTION_VISIBLE_CONFIRMED | rule_id=%s | target_id=%s | message_id=%s | session=%s | requested_reactions=%s | visible_reactions=%s",
                    rule_id,
                    entity,
                    sent_message_id,
                    session_name,
                    variant,
                    visible_reactions,
                )
            else:
                logger.warning(
                    "PREMIUM_REACTION_NOT_VISIBLE_AFTER_SEND | rule_id=%s | target_id=%s | message_id=%s | session=%s | requested_reactions=%s | visible_reactions=%s",
                    rule_id,
                    entity,
                    sent_message_id,
                    session_name,
                    variant,
                    visible_reactions,
                )
            return bool(confirmed)

        logger.warning(
            "Premium-реактор %s не смог поставить ни один вариант реакций на сообщение %s в %s. Последняя ошибка: %s",
            session_name,
            sent_message_id,
            entity,
            last_error,
        )
        return False

    async def _confirm_reaction(self, client, entity, message_id: int, emoji: str) -> bool:
        owner = self.owner
        try:
            normalized_expected = _normalize_reaction_emoji(emoji)
            msg = await client.get_messages(entity, ids=message_id)
            reactions = getattr(msg, "reactions", None)
            if not reactions:
                return False
            for result in getattr(reactions, "results", []) or []:
                reaction = getattr(result, "reaction", None)
                actual = _normalize_reaction_emoji(getattr(reaction, "emoticon", None))
                if actual == normalized_expected:
                    return True
            return False
        except Exception:
            return False

    async def _confirm_reaction_set(self, client, entity, message_id: int, emojis: list[str]) -> tuple[bool, list[str]]:
        owner = self.owner
        try:
            msg = await client.get_messages(entity, ids=message_id)
            reactions = getattr(msg, "reactions", None)
            if not reactions:
                logger.warning(
                    "CONFIRM_REACTION_SET_DEBUG | target_id=%s | message_id=%s | expected=%s | observed=%s",
                    entity,
                    message_id,
                    emojis,
                    {"reactions": None},
                )
                return False
            actual_emojis: set[str] = set()
            observed_results: list[dict[str, Any]] = []
            for result in getattr(reactions, "results", []) or []:
                reaction = getattr(result, "reaction", None)
                emoticon = getattr(reaction, "emoticon", None)
                observed_results.append(
                    {
                        "result_class": result.__class__.__name__,
                        "reaction_class": reaction.__class__.__name__ if reaction else None,
                        "emoticon": emoticon,
                        "document_id": getattr(reaction, "document_id", None) if reaction else None,
                        "count": getattr(result, "count", None),
                    }
                )
                normalized_emoticon = _normalize_reaction_emoji(emoticon)
                if normalized_emoticon:
                    actual_emojis.add(normalized_emoticon)
            observed = {
                "reactions_class": reactions.__class__.__name__,
                "results": observed_results,
                "recent_reactions_len": len(getattr(reactions, "recent_reactions", []) or []),
            }
            expected = {
                _normalize_reaction_emoji(emoji)
                for emoji in (emojis or [])
                if _normalize_reaction_emoji(emoji)
            }
            visible = sorted(actual_emojis)
            confirmed = bool(expected.intersection(actual_emojis))
            if not confirmed:
                logger.warning(
                    "CONFIRM_REACTION_SET_DEBUG | target_id=%s | message_id=%s | expected=%s | observed=%s",
                    entity,
                    message_id,
                    emojis,
                    observed,
                )
            return confirmed, visible
        except Exception:
            return False, []

    async def _select_reaction_message_id(self, target_id, sent_message_ids: list[int] | None) -> tuple[int | None, str]:
        owner = self.owner
        ids = sorted({int(x) for x in (sent_message_ids or []) if x is not None})
        if not ids:
            return None, "missing_sent_message_ids"

        entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
        try:
            fetched = await owner.telethon.get_messages(entity, ids=ids)
            fetched_list = fetched if isinstance(fetched, list) else [fetched]
            fetched_by_id = {int(m.id): m for m in fetched_list if m}
        except Exception:
            fetched_by_id = {}

        for mid in ids:
            msg = fetched_by_id.get(mid)
            if not msg:
                continue
            text_value = (
                getattr(msg, "raw_text", None)
                or getattr(msg, "text", None)
                or getattr(msg, "message", None)
                or ""
            ).strip()
            if text_value:
                return mid, "caption_message"

        return ids[0], "first_album_message"

    async def _add_reaction_if_possible(self, target_id, sent_message_id, rule_id: int | None = None):
        owner = self.owner
        if not owner.reaction_clients:
            return

        entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
        premium_reactors: list[ReactionClientInfo] = []
        normal_reactors: list[ReactionClientInfo] = []

        for reactor in owner.reaction_clients:
            if reactor.is_premium and reactor.fixed_reactions:
                premium_reactors.append(reactor)
            else:
                normal_reactors.append(reactor)

        premium_accepted = False

        for reactor in premium_reactors:
            try:
                premium_result = await self._try_add_premium_reactions(
                    client=reactor.client,
                    entity=entity,
                    sent_message_id=sent_message_id,
                    session_name=reactor.session_name,
                    rule_id=rule_id,
                    fixed_reactions=reactor.fixed_reactions,
                )
                if premium_result:
                    premium_accepted = True

            except Exception as exc:
                logger.warning(
                    "Реактор %s упал на сообщении %s в %s: %s",
                    reactor.session_name,
                    sent_message_id,
                    target_id,
                    exc,
                )

        if premium_accepted:
            logger.info(
                "REACTION_NORMAL_CONTINUE_AFTER_PREMIUM | rule_id=%s | target_id=%s | message_id=%s | normal_count=%s",
                rule_id,
                entity,
                sent_message_id,
                len(normal_reactors),
            )

        for reactor in normal_reactors:
            try:
                await self._try_add_normal_reaction(
                    client=reactor.client,
                    entity=entity,
                    sent_message_id=sent_message_id,
                    session_name=reactor.session_name,
                    rule_id=rule_id,
                )
            except Exception as exc:
                logger.warning(
                    "Реактор %s упал на сообщении %s в %s: %s",
                    reactor.session_name,
                    sent_message_id,
                    target_id,
                    exc,
                )

    async def _add_reaction_for_rule_if_possible(
        self,
        *,
        rule,
        target_id,
        sent_message_id,
        source_channel: str = "",
        source_message_ids: list[int] | None = None,
        delivery_id: int | None = None,
        max_age_seconds: int = 300,
    ) -> None:
        owner = self.owner
        rule_id = int(getattr(rule, "id", 0) or 0)
        if str(source_channel) and str(source_channel) == str(target_id):
            logger.info("SELF_TARGET_REPOST_DETECTED | rule_id=%s | source_id=%s | target_id=%s", rule_id, source_channel, target_id)
        validated_id = await self._validate_reaction_target_message(
            rule_id=rule_id,
            source_channel=str(source_channel or ""),
            target_id=str(target_id),
            source_message_ids=source_message_ids or [],
            sent_message_id=sent_message_id,
            delivery_id=delivery_id,
            max_age_seconds=max_age_seconds,
        )
        if not validated_id:
            return
        sent_message_id = validated_id

        resolver = ReactionRuntimeResolver(owner.db)

        try:
            plan = resolver.resolve_for_rule(rule)
        except Exception as exc:
            logger.warning(
                "REACTION_RUNTIME_RESOLVE_FAILED | rule_id=%s | error_type=%s",
                rule_id,
                exc.__class__.__name__,
            )
            await self._add_reaction_if_possible(target_id, sent_message_id, rule_id=rule_id)
            return

        if plan.use_legacy_reactors:
            logger.info(
                "REACTION_RUNTIME_SELECTED | rule_id=%s | mode=legacy_admin | reason=%s | legacy_clients=%s",
                rule_id,
                plan.reason,
                len(owner.reaction_clients or []),
            )
            await self._add_reaction_if_possible(target_id, sent_message_id, rule_id=rule_id)
            return

        if plan.mode == "disabled":
            logger.info(
                "REACTION_RUNTIME_SKIPPED | rule_id=%s | mode=disabled | reason=%s",
                rule_id,
                plan.reason,
            )
            return

        if plan.mode == "no_accounts":
            logger.info(
                "REACTION_RUNTIME_SKIPPED | rule_id=%s | mode=no_accounts | tenant_id=%s | reason=%s",
                rule_id,
                plan.tenant_id,
                plan.reason,
            )
            return

        if plan.use_tenant_reactors:
            logger.info(
                "REACTION_RUNTIME_SELECTED | rule_id=%s | mode=tenant_saas | tenant_id=%s | accounts=%s",
                rule_id,
                plan.tenant_id,
                len(plan.tenant_accounts),
            )
            account_ids = [int(a["id"]) for a in (plan.tenant_accounts or []) if a.get("id") is not None]
            try:
                job_id = await run_db(
                    owner.db.enqueue_reaction_job,
                    tenant_id=int(plan.tenant_id or 0),
                    rule_id=rule_id,
                    target_id=target_id,
                    message_id=int(sent_message_id),
                    account_ids=account_ids,
                    max_attempts=3,
                )
                logger.info(
                    "REACTION_JOB_ENQUEUED | tenant_id=%s | rule_id=%s | job_id=%s | target_id=%s | message_id=%s | accounts=%s",
                    plan.tenant_id,
                    rule_id,
                    job_id,
                    target_id,
                    sent_message_id,
                    len(account_ids),
                )
            except Exception:
                logger.exception("REACTION_JOB_ENQUEUE_FAILED | tenant_id=%s | rule_id=%s", plan.tenant_id, rule_id)
            return

