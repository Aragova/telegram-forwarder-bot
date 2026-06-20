"""Telegram update handlers for campaign invite link events."""

from __future__ import annotations

import logging

from aiogram import Router
from aiogram.types import ChatJoinRequest, ChatMemberUpdated

from app.campaign_invite_link_events_service import CampaignInviteLinkEventsService

logger = logging.getLogger(__name__)
router = Router(name="campaign_invite_link_updates")


@router.chat_join_request()
async def handle_campaign_invite_link_chat_join_request(
    chat_join_request: ChatJoinRequest,
    campaign_invite_link_repo=None,
):
    logger.debug("CAMPAIGN_INVITE_LINK_JOIN_REQUEST_RECEIVED")
    if campaign_invite_link_repo is None:
        logger.debug("CAMPAIGN_INVITE_LINK_JOIN_REQUEST_SKIPPED reason=missing_repo")
        return
    try:
        service = CampaignInviteLinkEventsService(repo=campaign_invite_link_repo, logger=logger)
        result = await service.handle_chat_join_request(chat_join_request)
        if result.get("ok"):
            logger.info("CAMPAIGN_INVITE_LINK_JOIN_REQUEST_RECORDED invite_link_id=%s rule_id=%s", result.get("invite_link_id"), result.get("rule_id"))
        else:
            logger.debug("CAMPAIGN_INVITE_LINK_JOIN_REQUEST_SKIPPED reason=%s", result.get("reason"))
    except Exception as exc:
        logger.warning("CAMPAIGN_INVITE_LINK_JOIN_REQUEST_FAILED error=%s", exc)


@router.chat_member()
async def handle_campaign_invite_link_chat_member(
    chat_member: ChatMemberUpdated,
    campaign_invite_link_repo=None,
):
    logger.debug("CAMPAIGN_INVITE_LINK_CHAT_MEMBER_RECEIVED")
    if campaign_invite_link_repo is None:
        logger.debug("CAMPAIGN_INVITE_LINK_CHAT_MEMBER_SKIPPED reason=missing_repo")
        return
    try:
        service = CampaignInviteLinkEventsService(repo=campaign_invite_link_repo, logger=logger)
        result = await service.handle_chat_member_updated(chat_member)
        if result.get("ok"):
            logger.info("CAMPAIGN_INVITE_LINK_CHAT_MEMBER_RECORDED event_type=%s invite_link_id=%s rule_id=%s", result.get("event_type"), result.get("invite_link_id"), result.get("rule_id"))
        else:
            logger.debug("CAMPAIGN_INVITE_LINK_CHAT_MEMBER_SKIPPED reason=%s", result.get("reason"))
    except Exception as exc:
        logger.warning("CAMPAIGN_INVITE_LINK_CHAT_MEMBER_FAILED error=%s", exc)
