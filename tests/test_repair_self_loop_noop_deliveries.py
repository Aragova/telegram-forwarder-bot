from scripts.repair_self_loop_noop_deliveries import NOOP_METHODS, apply_repair, filter_candidates


ROWS = [
    {"delivery_id": 453993, "rule_id": 14, "source_message_id": 476, "noop_method": "self_loop_noop_single", "sent_at": "t", "status": "sent"},
    {"delivery_id": 2, "rule_id": 14, "source_message_id": 477, "noop_method": "reupload_single", "sent_at": "t", "status": "sent"},
    {"delivery_id": 3, "rule_id": 15, "source_message_id": 10, "noop_method": "self_loop_noop_album", "sent_at": "t", "status": "sent"},
    {"delivery_id": 4, "rule_id": 14, "source_message_id": 11, "noop_method": "self_loop_noop_single", "sent_at": None, "status": "pending"},
]


class FakeCursor:
    def __init__(self):
        self.calls = []
        self.rowcount = 0
        self.repaired = set()

    def execute(self, sql, params):
        self.calls.append((sql, params))
        if sql.lstrip().startswith("UPDATE deliveries"):
            delivery_id = int(params[0])
            if delivery_id in self.repaired:
                self.rowcount = 0
            else:
                self.repaired.add(delivery_id)
                self.rowcount = 1
        else:
            self.rowcount = 1


def test_dry_run_filter_does_not_mutate():
    rows = filter_candidates(ROWS)
    assert [r["delivery_id"] for r in rows] == [453993, 3]
    assert ROWS[0]["status"] == "sent"


def test_apply_repairs_only_noop_deliveries():
    cur = FakeCursor()
    rows = filter_candidates(ROWS)
    assert apply_repair(cur, rows) == 2
    update_ids = [params[0] for sql, params in cur.calls if sql.lstrip().startswith("UPDATE deliveries")]
    assert update_ids == [453993, 3]
    assert all(method in NOOP_METHODS for method in cur.calls[0][1][1])


def test_non_noop_sent_deliveries_not_selected():
    rows = filter_candidates(ROWS)
    assert 2 not in [r["delivery_id"] for r in rows]


def test_apply_is_idempotent():
    cur = FakeCursor()
    rows = filter_candidates(ROWS[:1])
    assert apply_repair(cur, rows) == 1
    assert apply_repair(cur, rows) == 0


def test_filter_can_limit_rule_id():
    rows = filter_candidates(ROWS, rule_id=14)
    assert [r["delivery_id"] for r in rows] == [453993]
