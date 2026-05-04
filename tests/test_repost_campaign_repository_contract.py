from app.postgres_repository import PostgresRepository


def test_repost_campaign_repository_methods_exist():
    repo = PostgresRepository()

    assert hasattr(repo, "get_rule_repost_campaign_summary")
    assert hasattr(repo, "update_rule_repost_campaign_settings")
    assert hasattr(repo, "add_rule_repost_campaign_target")
    assert hasattr(repo, "list_rule_repost_campaign_targets")
    assert hasattr(repo, "remove_rule_repost_campaign_target")
    assert hasattr(repo, "set_rule_repost_campaign_target_active")
