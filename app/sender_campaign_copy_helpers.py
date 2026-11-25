from __future__ import annotations

from .runtime_utils import run_db
from .send_intent import PublicationIntent, publication_scope, _definite_rejection
from .telegram_publication import botapi_publication_ids

def _unverified_copy():
    return {"ok":False,"accepted":True,"retryable":False,"manual_review_required":True,
            "error_text":"Telegram мог принять копию; ID не подтверждён. Повтор запрещён."}


class SenderCampaignCopyHelpers:
    def __init__(self, owner):
        self.owner = owner

    async def execute_send_copy_from_job(self, *, copy_id: int, **kwargs) -> dict:
        copy_row = await run_db(self.owner.db.get_delivery_campaign_copy, int(copy_id))
        if not copy_row:
            return {"ok": False, "retryable": False, "error_text": "Копия кампании не найдена"}
        if str(copy_row.get("send_status") or "") == "sent":
            return {"ok": True, "already_sent": True}
        if copy_row.get("send_status") in {"processing","unverified"}:
            return _unverified_copy()
        delivery = await run_db(self.owner.db.get_delivery, int(copy_row.get("delivery_id") or 0))
        if not delivery:
            await run_db(self.owner.db.mark_delivery_campaign_copy_send_failed, int(copy_id), "Delivery не найден")
            return {"ok": False, "retryable": False, "error_text": "Delivery не найден"}
        if str(delivery.get("delivery_method") or "") == "album":
            return {"ok": False, "retryable": False, "error_text": "Кампании для альбомов пока не поддерживаются в MVP"}
        rule = await run_db(self.owner.db.get_rule, int(copy_row.get("rule_id") or 0))
        show_seconds = int(getattr(rule, "repost_campaign_show_seconds", 0) or 0)
        if not await run_db(self.owner.db.mark_delivery_campaign_copy_processing,int(copy_id)):
            return _unverified_copy()
        topic = copy_row.get("target_thread_id")
        try:
            with publication_scope(PublicationIntent(self.owner.db,str(copy_id),kind="campaign_copy")):
                sent = await self.owner.bot.copy_message(chat_id=str(copy_row.get("target_id")),
                    from_chat_id=str(delivery.get("source_channel")),message_id=int(delivery.get("message_id")),
                    **({"message_thread_id":topic} if topic is not None else {}))
        except Exception as exc:
            if _definite_rejection(exc):
                await run_db(self.owner.db.mark_delivery_campaign_copy_send_failed,int(copy_id),str(exc))
                raise
            await run_db(self.owner.db.mark_delivery_campaign_copy_unverified,int(copy_id),"copy_rpc_outcome_unknown")
            return _unverified_copy()
        sent_ids = botapi_publication_ids(sent,1)
        if not sent_ids:
            await run_db(self.owner.db.mark_delivery_campaign_copy_unverified,int(copy_id),"botapi_accepted_without_published_id")
            return _unverified_copy()
        from app.repost_campaign_service import build_campaign_delete_after_iso
        from app.job_service import enqueue_repost_campaign_delete_copy
        delete_after_at = build_campaign_delete_after_iso(show_seconds)
        if not await run_db(self.owner.db.mark_delivery_campaign_copy_sent, int(copy_id), sent_message_id=sent_ids[0], sent_message_ids=sent_ids, delivery_method="copy_single", delete_after_at=delete_after_at):
            return _unverified_copy()
        await run_db(enqueue_repost_campaign_delete_copy, self.owner.db, int(copy_id), run_at=delete_after_at)
        return {"ok": True, "copy_id": int(copy_id), "sent_message_ids": sent_ids}

    async def execute_delete_copy_from_job(self, *, copy_id: int, **kwargs) -> dict:
        copy_row = await run_db(self.owner.db.get_delivery_campaign_copy, int(copy_id))
        if not copy_row:
            return {"ok": False, "retryable": False, "error_text": "Копия кампании не найдена"}
        if str(copy_row.get("delete_status") or "") in {"deleted", "skipped"}:
            return {"ok": True, "already_done": True}
        msg_ids = copy_row.get("sent_message_ids") or []
        if not msg_ids and copy_row.get("sent_message_id"):
            msg_ids = [int(copy_row.get("sent_message_id"))]
        if not msg_ids:
            await run_db(self.owner.db.mark_delivery_campaign_copy_delete_skipped, int(copy_id), "Нет message_id для удаления")
            return {"ok": True}
        await run_db(self.owner.db.mark_delivery_campaign_copy_delete_processing, int(copy_id))
        for mid in msg_ids:
            try:
                await self.owner.bot.delete_message(chat_id=str(copy_row.get("target_id")), message_id=int(mid))
            except Exception as exc:
                text = str(exc)
                if "not found" in text.lower() or "message to delete not found" in text.lower():
                    continue
                await run_db(self.owner.db.mark_delivery_campaign_copy_delete_failed, int(copy_id), text)
                return {"ok": False, "retryable": ("retry after" in text.lower()), "error_text": text}
        await run_db(self.owner.db.mark_delivery_campaign_copy_deleted, int(copy_id))
        return {"ok": True}
