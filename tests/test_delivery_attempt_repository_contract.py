from app.delivery_idempotency import extract_sent_message_ids_from_attempt


class FakeAttemptRepo:
    def __init__(self):
        self.rows = {}
        self.next_id = 1

    def get_delivery_attempt_by_idempotency_key(self, idempotency_key):
        return self.rows.get(idempotency_key)

    def create_delivery_attempt(self, **kwargs):
        key = kwargs["idempotency_key"]
        if key in self.rows:
            return self.rows[key]["id"]
        row = {"id": self.next_id, **kwargs, "sent_message_ids_json": kwargs.get("sent_message_ids")}
        self.rows[key] = row
        self.next_id += 1
        return row["id"]

    def mark_delivery_attempt_sending(self, idempotency_key, **kwargs):
        row = self.rows.get(idempotency_key)
        if not row or row.get("status") not in {"created", "failed_before_send"}:
            return False
        row["status"] = "sending"
        row["job_id"] = kwargs.get("job_id")
        return True

    def mark_delivery_attempt_accepted(self, idempotency_key, *, sent_message_ids, telegram_method=None):
        row = self.rows.get(idempotency_key)
        if not row:
            return False
        row["status"] = "accepted"
        row["sent_message_ids_json"] = sent_message_ids
        row["telegram_method"] = telegram_method
        return True


def test_delivery_attempt_contract():
    repo = FakeAttemptRepo()
    key = "delivery:1:target:t:single"
    created_id = repo.create_delivery_attempt(delivery_id=1, rule_id=1, tenant_id=1, job_id=10, idempotency_key=key, operation_kind="single", status="created")
    assert created_id == 1
    second_id = repo.create_delivery_attempt(delivery_id=1, rule_id=1, tenant_id=1, job_id=10, idempotency_key=key, operation_kind="single", status="created")
    assert second_id == created_id
    assert repo.mark_delivery_attempt_sending(key, job_id=10) is True
    assert repo.mark_delivery_attempt_accepted(key, sent_message_ids=[11, 12], telegram_method="copy_single") is True
    assert repo.mark_delivery_attempt_sending(key, job_id=11) is False
    attempt = repo.get_delivery_attempt_by_idempotency_key(key)
    assert extract_sent_message_ids_from_attempt(attempt) == [11, 12]
