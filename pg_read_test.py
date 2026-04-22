from __future__ import annotations

from app.postgres_repository import PostgresRepository


def main() -> None:
    repo = PostgresRepository()

    print("PING:", repo.ping_backend())
    print("QUEUE_STATS:", repo.get_queue_stats())
    print("INTROS:", repo.get_intros())
    print("RECENT_AUDIT_COUNT:", len(repo.get_recent_audit(10)))

    channels = repo.get_channels()
    print("CHANNELS_COUNT:", len(channels))

    source_channels = repo.get_channels("source")
    print("SOURCE_CHANNELS_COUNT:", len(source_channels))

    target_channels = repo.get_channels("target")
    print("TARGET_CHANNELS_COUNT:", len(target_channels))

    rules = repo.get_all_rules()
    print("RULES_COUNT:", len(rules))

    if rules:
        first_rule = repo.get_rule(rules[0].id)
        print("FIRST_RULE_ID:", first_rule.id if first_rule else None)
        print("FIRST_RULE_MODE:", first_rule.mode if first_rule else None)
    else:
        print("FIRST_RULE_ID:", None)
        print("FIRST_RULE_MODE:", None)


if __name__ == "__main__":
    main()
