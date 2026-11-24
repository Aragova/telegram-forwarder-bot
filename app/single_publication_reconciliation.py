"""Durable reconciliation of a single MTProto scheduled-video receipt.

No transport send is permitted anywhere in this module.
"""
from __future__ import annotations

import logging
from typing import Any

from .runtime_utils import run_db
from .telethon_authoritative_resolver import TelethonAuthoritativeMessageResolver

logger = logging.getLogger("forwarder.publication")


def _pending() -> dict[str, Any]:
    return {"ok": False, "accepted": True, "retryable": False, "publication_pending": True}


def _manual(reason: str) -> dict[str, Any]:
    return {"ok": False, "accepted": True, "retryable": False,
            "manual_review_required": True, "error_text": reason}


async def register_pending_single(owner, *, key, delivery_id, rule_id, target_id,
                                  target_thread_id, source_channel, source_message_id,
                                  scheduled_id) -> bool:
    inbox = getattr(owner.telethon, "publication_inbox", None)
    register = getattr(owner.db, "register_single_publication_receipt", None)
    if not key or not scheduled_id or inbox is None or not callable(register):
        return False
    try:
        return bool(await run_db(
            register, key=key, delivery_id=int(delivery_id), rule_id=int(rule_id),
            account_id=inbox.sender_account_id, peer_id=int(target_id),
            thread_id=target_thread_id, source_channel=str(source_channel),
            source_message_id=int(source_message_id), scheduled_id=int(scheduled_id),
        ))
    except Exception:
        logger.exception("SINGLE_PUBLICATION_RECEIPT_PERSIST_FAILED | delivery_id=%s", delivery_id)
        # Original caller retains its existing terminal/no-resend path.
        return False


async def reconcile_pending_single(owner, key: str) -> dict[str, Any] | None:
    get_receipt = getattr(owner.db, "get_single_publication_receipt", None)
    if not callable(get_receipt):
        return None
    receipt = await run_db(get_receipt, key)
    if not isinstance(receipt, dict):
        return None

    inbox = getattr(owner.telethon, "publication_inbox", None)
    if receipt["published_id"] is None and (inbox is None or inbox.sender_account_id != receipt["sender_account_id"]):
        if not receipt["expired"]:
            return _pending()
        return await _expire(owner, receipt, key, "publication_sender_unavailable")

    target_id = receipt["target_peer_id"]
    source_ids = [int(receipt["source_message_id"])]
    source_guard = set(source_ids) if str(receipt["source_channel"]) == str(target_id) else None
    published_id = receipt["published_id"]
    if published_id is None:
        try:
            entity = await owner.telethon.get_entity(target_id)
            resolver = TelethonAuthoritativeMessageResolver(owner.telethon)
            published_id = await resolver.resolve_scheduled_publication(
                target_entity=entity, target_id=target_id,
                scheduled_id=receipt["scheduled_id"], source_message_ids=source_guard,
                target_thread_id=receipt["target_thread_id"], before_max_message_id=None,
                send_started_at=receipt["send_started_at"],
                target_is_broadcast_channel=bool(getattr(entity, "broadcast", False)
                                                 and not getattr(entity, "megagroup", False)),
            )
        except Exception as exc:
            logger.warning("SINGLE_PUBLICATION_READ_FAILED | delivery_id=%s | error_type=%s",
                           receipt["delivery_id"], type(exc).__name__)
            published_id = None

        if published_id is None:
            if receipt["expired"]:
                return await _expire(owner, receipt, key, "publication_mapping_unavailable")
            return _pending()
        if not await run_db(owner.db.finalize_single_publication_receipt, key, published_id):
            # A competing execution may have committed. Re-read, never resend.
            receipt = await run_db(get_receipt, key)
            if not receipt or receipt.get("published_id") != published_id:
                if receipt and receipt["expired"]:
                    return await _expire(owner, receipt, key, "publication_finalize_conflict")
                return _pending()
        else:
            receipt = await run_db(get_receipt, key)

    if not receipt or receipt.get("published_id") != published_id:
        return _pending()

    state = receipt["reaction_state"]
    if state == "dispatching":
        if not receipt["dispatch_stale"]:
            return _pending()
        # Legacy reaction RPC has no durable request key. Never re-run a
        # partially completed fanout blindly after a process crash.
        await run_db(owner.db.finish_single_publication_reaction, key, "uncertain",
                     {"reason": "interrupted_reaction_dispatch"})
    elif state == "pending":
        if not await run_db(owner.db.claim_single_publication_reaction, key):
            return _pending()
        try:
            rule = await run_db(owner.db.get_rule, receipt["rule_id"])
            if (rule is None or str(rule.target_id) != str(target_id)
                    or rule.target_thread_id != receipt["target_thread_id"]):
                result = {"reason": "rule_target_changed"}
            else:
                result = await owner._add_reaction_for_rule_if_possible(
                    rule=rule, target_id=str(target_id), sent_message_id=published_id,
                    source_channel=receipt["source_channel"], source_message_ids=source_ids,
                    delivery_id=receipt["delivery_id"], max_age_seconds=86400,
                )
            await run_db(owner.db.finish_single_publication_reaction, key, "done",
                         result if isinstance(result, dict) else {"applied": bool(result)})
        except Exception as exc:
            await run_db(owner.db.finish_single_publication_reaction, key, "uncertain",
                         {"reason": type(exc).__name__})
            logger.warning("SINGLE_PUBLICATION_REACTION_UNCERTAIN | delivery_id=%s", receipt["delivery_id"])

    return {"ok": True, "accepted": True, "sent_message_ids": [published_id]}


async def _expire(owner, receipt, key, reason):
    # Retain evidence and accepted ledger; no automatic repeat after deadline.
    await run_db(owner.db.mark_delivery_faulty_if_attempt_unverified,
                 receipt["delivery_id"], key, reason)
    return _manual(reason)
