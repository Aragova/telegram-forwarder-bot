from app.postgres_repository import PostgresRepository


def test_repost_campaign_repository_methods_exist():
    repo = PostgresRepository()

    assert hasattr(repo, "get_rule_repost_campaign_summary")
    assert hasattr(repo, "update_rule_repost_campaign_settings")
    assert hasattr(repo, "add_rule_repost_campaign_target")
    assert hasattr(repo, "list_rule_repost_campaign_targets")
    assert hasattr(repo, "remove_rule_repost_campaign_target")
    assert hasattr(repo, "set_rule_repost_campaign_target_active")
    required = [
        "create_campaign_run",
        "update_campaign_run_status",
        "create_campaign_run_message",
        "mark_campaign_run_message_sending",
        "mark_campaign_run_message_sent",
        "mark_campaign_run_message_failed",
        "claim_due_campaign_run_messages_for_delete",
        "mark_campaign_run_message_deleted",
        "mark_campaign_run_message_delete_failed",
        "reset_stuck_campaign_delete_processing",
        "get_campaign_run",
        "list_campaign_runs_for_rule",
        "list_campaign_run_messages",
    ]
    for name in required:
        assert hasattr(repo, name)


def test_row_to_rule_maps_repost_campaign_fields():
    repo = PostgresRepository()
    row = {
        "id": 1,
        "source_id": "-1001",
        "source_thread_id": None,
        "target_id": "-1002",
        "target_thread_id": None,
        "interval": 3600,
        "schedule_mode": "interval",
        "fixed_times_json": None,
        "is_active": True,
        "created_date": "2026-05-04T00:00:00+00:00",
        "next_run_at": None,
        "last_sent_at": None,
        "source_title": "Source",
        "target_title": "Target",
        "mode": "repost",
        "video_trim_seconds": 120,
        "video_clip_duration_seconds": 118,
        "video_add_intro": False,
        "video_intro_horizontal": None,
        "video_intro_vertical": None,
        "video_intro_horizontal_id": None,
        "video_intro_vertical_id": None,
        "video_caption": None,
        "video_caption_entities_json": None,
        "caption_delivery_mode": "auto",
        "video_caption_delivery_mode": "auto",
        "repost_campaign_enabled": True,
        "repost_campaign_show_seconds": 86400,
    }

    rule = repo._row_to_rule(row)

    assert rule.repost_campaign_enabled is True
    assert rule.repost_campaign_show_seconds == 86400



def test_saved_posts_repository_methods_exist():
    repo = PostgresRepository()

    assert hasattr(repo, "create_saved_post")
    assert hasattr(repo, "get_saved_post")
    assert hasattr(repo, "list_saved_posts")
    assert hasattr(repo, "update_saved_post_content")
    assert hasattr(repo, "archive_saved_post")
    assert hasattr(repo, "set_rule_repost_campaign_saved_post")
