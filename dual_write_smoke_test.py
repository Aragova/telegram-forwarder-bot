from __future__ import annotations

from app.dual_write_repository import DualWriteRepository


def main() -> None:
    repo = DualWriteRepository()

    print("QUEUE_STATS:", repo.get_queue_stats())

    problem = repo.register_problem(
        problem_key="dual_write_test_problem",
        problem_type="test",
        rule_id=None,
        delivery_id=None,
        extra={"source": "dual_write_smoke_test"},
    )
    print("REGISTER_PROBLEM_OK:", bool(problem))

    resolved = repo.resolve_problem("dual_write_test_problem")
    print("RESOLVE_PROBLEM_RESULT:", resolved)

    notified = repo.mark_problem_notified("dual_write_test_problem")
    print("MARK_NOTIFIED_RESULT:", notified)

    print("CHANNELS_COUNT:", len(repo.get_channels()))
    print("RULES_COUNT:", len(repo.get_all_rules()))

    # runtime smoke
    sample = repo.get_faulty_deliveries(limit=1)
    print("FAULTY_SAMPLE_COUNT:", len(sample))

    if sample:
        delivery_id = int(sample[0]["id"])
        rule_id = int(sample[0]["rule_id"])
        post_id = int(sample[0]["post_id"])

        repo.log_delivery_event(
            event_type="dual_write_runtime_test",
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            status="test",
            error_text=None,
            extra={"source": "dual_write_smoke_test"},
        )
        print("LOG_DELIVERY_EVENT_OK:", True)
    else:
        print("LOG_DELIVERY_EVENT_OK:", "SKIPPED_NO_FAULTY")

    print("DUAL_WRITE_SMOKE_OK")


if __name__ == "__main__":
    main()
