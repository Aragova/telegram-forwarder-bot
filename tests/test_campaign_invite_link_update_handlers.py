from pathlib import Path


def test_router_has_join_request_and_chat_member_handlers():
    source = Path("app/campaign_invite_link_update_handlers.py").read_text()
    assert "router = Router" in source
    assert "@router.chat_join_request()" in source
    assert "@router.chat_member()" in source


def test_no_join_request_approve_or_decline_actions_added():
    combined = "\n".join(Path(path).read_text() for path in ["app/campaign_invite_link_update_handlers.py", "app/campaign_invite_link_events_service.py"])
    forbidden = ["approve_chat_join_request", "decline_chat_join_request", "approveChatJoinRequest", "declineChatJoinRequest"]
    for token in forbidden:
        assert token not in combined


def test_router_registered_in_startup():
    source = Path("bot.py").read_text()
    assert "campaign_invite_link_update_router" in source
    assert "dp.include_router(campaign_invite_link_update_router)" in source
    assert 'dp["campaign_invite_link_repo"] = db' in source


def test_allowed_updates_do_not_block_join_request_or_chat_member():
    source = Path("bot.py").read_text()
    assert "allowed_updates" not in source or "chat_join_request" in source
    assert "allowed_updates" not in source or "chat_member" in source
