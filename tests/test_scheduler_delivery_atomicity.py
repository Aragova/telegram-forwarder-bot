from app.job_service import build_dedup_key_for_single


class FakeAtomicRepo:
    def __init__(self):
        self.next_run_at = '2026-01-01T00:00:00+00:00'
        self.delivery_status = 'pending'
        self.jobs = []

    def take_due_delivery_and_create_job(self, _rule_id, due_iso):
        if self.next_run_at > due_iso:
            return None
        dedup = build_dedup_key_for_single(1)
        for job in self.jobs:
            if job['dedup_key'] == dedup and job['status'] in {'pending', 'leased', 'processing', 'retry'}:
                return {'status': 'duplicate', 'delivery_id': 1, 'dedup_key': dedup}
        self.delivery_status = 'processing'
        self.next_run_at = '2099-01-01T00:00:00+00:00'
        self.jobs.append({'id': 1, 'dedup_key': dedup, 'status': 'pending'})
        return {'status': 'created', 'job_id': 1, 'delivery_id': 1, 'dedup_key': dedup}


def test_two_calls_do_not_create_two_jobs_for_same_slot():
    repo = FakeAtomicRepo()
    first = repo.take_due_delivery_and_create_job(89, '2026-05-03T00:00:00+00:00')
    second = repo.take_due_delivery_and_create_job(89, '2026-05-03T00:00:00+00:00')
    assert first and first['status'] == 'created'
    assert second is None
    assert len(repo.jobs) == 1


def test_active_dedup_prevents_new_job():
    repo = FakeAtomicRepo()
    repo.jobs.append({'id': 42, 'dedup_key': build_dedup_key_for_single(1), 'status': 'processing'})
    result = repo.take_due_delivery_and_create_job(89, '2026-05-03T00:00:00+00:00')
    assert result and result['status'] == 'duplicate'
    assert len(repo.jobs) == 1
