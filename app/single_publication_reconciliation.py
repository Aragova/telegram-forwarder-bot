"""Shared durable reconciliation of MTProto scheduled publication receipts.

A receipt may represent one video, a single repost, or a complete album.
This module never issues a Telegram send or re-upload.
"""
from __future__ import annotations

import logging
from typing import Any

from .runtime_utils import run_db
from .telethon_authoritative_resolver import TelethonAuthoritativeMessageResolver

logger = logging.getLogger("forwarder.publication")


def _pending() -> dict[str, Any]:
    return {"ok": False,"accepted": True,"retryable": False,"publication_pending": True}


def _manual(reason: str) -> dict[str, Any]:
    return {"ok": False,"accepted": True,"retryable": False,
            "manual_review_required": True,"error_text": reason}


async def register_pending_publication(owner, *, key, delivery_ids, rule_id, target_id,
                                       target_thread_id, source_channel, source_message_ids,
                                       scheduled_ids) -> bool:
    inbox = getattr(owner.telethon,"publication_inbox",None)
    register = getattr(owner.db,"register_publication_receipt",None)
    if not key or inbox is None or not callable(register):
        return False
    try:
        return bool(await run_db(
            register,key=key,delivery_ids=[int(i) for i in delivery_ids],rule_id=int(rule_id),
            account_id=inbox.sender_account_id,peer_id=int(target_id),
            thread_id=target_thread_id,source_channel=str(source_channel),
            source_message_ids=[int(i) for i in source_message_ids],
            scheduled_ids=[int(i) for i in scheduled_ids],
        ))
    except Exception:
        logger.exception("PUBLICATION_RECEIPT_PERSIST_FAILED | delivery_ids=%s",delivery_ids)
        return False


async def register_pending_single(owner, *, key, delivery_id, rule_id, target_id,
                                  target_thread_id, source_channel, source_message_id,
                                  scheduled_id) -> bool:
    return await register_pending_publication(
        owner,key=key,delivery_ids=[delivery_id],rule_id=rule_id,target_id=target_id,
        target_thread_id=target_thread_id,source_channel=source_channel,
        source_message_ids=[source_message_id],scheduled_ids=[scheduled_id],
    )


async def reconcile_pending_publication(owner,key: str) -> dict[str,Any] | None:
    get_receipt = getattr(owner.db,"get_publication_receipt",None)
    if not callable(get_receipt):
        return None
    receipt = await run_db(get_receipt,key)
    if not isinstance(receipt,dict):
        return None
    if receipt.get("expired_at"):
        return _manual("publication_receipt_expired_no_resend")
    if receipt["published_ids"] is None and receipt.get("expired"):
        return await _wait_or_expire(owner,receipt,key,"publication_receipt_expired_no_resend")

    inbox = getattr(owner.telethon,"publication_inbox",None)
    if receipt["published_ids"] is None and (inbox is None or inbox.sender_account_id != receipt["sender_account_id"]):
        return await _wait_or_expire(owner,receipt,key,"publication_sender_unavailable")

    target_id = receipt["target_peer_id"]
    source_ids = receipt["source_message_ids"]
    source_guard = set(source_ids) if receipt["source_channel"] == str(target_id) else None
    published_ids = receipt["published_ids"]
    if published_ids is None:
        try:
            entity = await owner.telethon.get_entity(target_id)
            resolver = TelethonAuthoritativeMessageResolver(owner.telethon)
            published_ids = await resolver.resolve_scheduled_publications(
                target_entity=entity,target_id=target_id,scheduled_ids=receipt["scheduled_ids"],
                source_message_ids=source_guard,target_thread_id=receipt["target_thread_id"],
                before_max_message_id=None,send_started_at=receipt["send_started_at"],
                target_is_broadcast_channel=bool(getattr(entity,"broadcast",False)
                                                 and not getattr(entity,"megagroup",False)),
            )
        except Exception as exc:
            logger.warning("PUBLICATION_READ_FAILED | delivery_ids=%s | error_type=%s",
                           receipt["delivery_ids"],type(exc).__name__)
            published_ids = []
        if not published_ids:
            return await _wait_or_expire(owner,receipt,key,"publication_mapping_unavailable")
        await run_db(owner.db.finalize_publication_receipt,key,published_ids)
        receipt = await run_db(get_receipt,key)
        if not receipt or receipt.get("published_ids") != published_ids:
            return await _wait_or_expire(owner,receipt,key,"publication_finalize_conflict")

    state = receipt["reaction_state"]
    if state == "dispatching":
        if not receipt["dispatch_stale"]:
            return _pending()
        await run_db(owner.db.finish_publication_reaction,key,"uncertain",
                     {"reason":"interrupted_reaction_dispatch"})
    elif state == "pending":
        if not await run_db(owner.db.claim_publication_reaction,key):
            return _pending()
        try:
            rule = await run_db(owner.db.get_rule,receipt["rule_id"])
            if (rule is None or str(rule.target_id) != str(target_id)
                    or rule.target_thread_id != receipt["target_thread_id"]
                    or getattr(rule,"tenant_id",receipt["tenant_id"]) != receipt["tenant_id"]):
                result = {"reason":"rule_target_changed"}
            else:
                reaction_id = published_ids[0]
                if len(published_ids)>1:
                    reaction_id,_ = await owner._select_reaction_message_id(str(target_id),published_ids)
                    if reaction_id not in published_ids:
                        reaction_id = None
                if reaction_id is None:
                    result = {"reason":"album_reaction_target_unavailable"}
                else:
                    result = await owner._add_reaction_for_rule_if_possible(
                        rule=rule,target_id=str(target_id),sent_message_id=reaction_id,
                        source_channel=receipt["source_channel"],source_message_ids=source_ids,
                        delivery_id=receipt["delivery_ids"][0],max_age_seconds=86400,
                    )
            result = result if isinstance(result,dict) else {"applied":bool(result)}
            completed = bool(result.get("applied") or result.get("enqueued") or result.get("reason") in {"disabled","no_accounts","rule_target_changed"})
            await run_db(owner.db.finish_publication_reaction,key,"done" if completed else "uncertain",result)
        except Exception as exc:
            await run_db(owner.db.finish_publication_reaction,key,"uncertain",
                         {"reason":type(exc).__name__})
            logger.warning("PUBLICATION_REACTION_UNCERTAIN | delivery_ids=%s",receipt["delivery_ids"])
    return {"ok":True,"accepted":True,"sent_message_ids":published_ids}


async def _wait_or_expire(owner,receipt,key,reason):
    if receipt and (receipt.get("expired") or receipt.get("expired_at")):
        await run_db(owner.db.expire_publication_receipt,key,reason)
        return _manual(reason)
    return _pending()


# Compatibility name for existing single-video callers; all use one implementation.
reconcile_pending_single = reconcile_pending_publication
