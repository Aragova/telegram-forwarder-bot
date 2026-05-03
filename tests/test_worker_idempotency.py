from app.worker_runtime import _InFlightState, _process_job


class DummySub:
    def get_active_subscription(self, _tenant_id):
        return {}


class DummyUsage:
    def get_today_usage(self, _tenant_id):
        return {}
    def increment_video(self, *_args, **_kwargs):
        return None


class FakeRepo:
    def __init__(self, status='processing'):
        self.delivery_status = {1: status, 2: 'processing', 3: 'processing'}
        self.completed = []
        self.retried = []
        self.failed = []

    def get_rule_tenant_id(self, _rule_id):
        return 1

    def mark_job_processing(self, *_args):
        return True

    def get_delivery(self, delivery_id):
        return {"id": delivery_id, "status": self.delivery_status.get(delivery_id)}

    def complete_job(self, job_id):
        self.completed.append(job_id)
        return True

    def retry_job(self, job_id, error, delay):
        self.retried.append((job_id, error, delay))
        return True

    def fail_job(self, job_id, error):
        self.failed.append((job_id, error))
        return True


class FakeSender:
    def __init__(self, repo, mode='ok_true'):
        self.repo = repo
        self.mode = mode
        self.calls = 0

    async def execute_repost_single_from_job(self, **_payload):
        self.calls += 1
        if self.mode == 'should_not_call':
            raise AssertionError('sender called unexpectedly')
        if self.mode == 'ok_false_after_sent':
            self.repo.delivery_status[1] = 'sent'
            return {"ok": False, "error_text": "fake error"}
        if self.mode == 'exception_after_sent':
            self.repo.delivery_status[1] = 'sent'
            raise RuntimeError('boom')
        return {"ok": True}

    async def execute_repost_album_from_job(self, **_payload):
        self.calls += 1
        raise AssertionError('album sender should not be called')

    async def execute_video_send_from_job(self, **_payload):
        self.calls += 1
        raise AssertionError('video sender should not be called')


def test_repost_single_already_sent_before_execution():
    repo = FakeRepo(status='sent')
    sender = FakeSender(repo, mode='should_not_call')
    job = {"id": 10, "job_type": "repost_single", "payload_json": {"delivery_id": 1, "rule_id": 1}}
    ok = __import__("asyncio").run(_process_job(repo, sender, 'w', 'light', job, policy=__import__('app.worker_resource_policy', fromlist=['POLICY']).POLICY, state=_InFlightState()))
    assert ok is True
    assert sender.calls == 0
    assert repo.completed == [10]
    assert repo.retried == []
    assert repo.failed == []


def test_repost_single_ok_false_but_delivery_became_sent():
    repo = FakeRepo(status='processing')
    sender = FakeSender(repo, mode='ok_false_after_sent')
    job = {"id": 11, "job_type": "repost_single", "payload_json": {"delivery_id": 1, "rule_id": 1}}
    ok = __import__("asyncio").run(_process_job(repo, sender, 'w', 'light', job, policy=__import__('app.worker_resource_policy', fromlist=['POLICY']).POLICY, state=_InFlightState()))
    assert ok is True
    assert sender.calls == 1
    assert repo.completed == [11]
    assert repo.retried == []
    assert repo.failed == []


def test_repost_single_exception_but_delivery_became_sent():
    repo = FakeRepo(status='processing')
    sender = FakeSender(repo, mode='exception_after_sent')
    job = {"id": 12, "job_type": "repost_single", "payload_json": {"delivery_id": 1, "rule_id": 1}}
    ok = __import__("asyncio").run(_process_job(repo, sender, 'w', 'light', job, policy=__import__('app.worker_resource_policy', fromlist=['POLICY']).POLICY, state=_InFlightState()))
    assert ok is True
    assert sender.calls == 1
    assert repo.completed == [12]
    assert repo.retried == []
    assert repo.failed == []


def test_repost_album_partial_sent_skips_send():
    repo = FakeRepo(status='processing')
    repo.delivery_status[1] = 'sent'
    sender = FakeSender(repo)
    job = {"id": 13, "job_type": "repost_album", "payload_json": {"delivery_ids": [1, 2, 3], "rule_id": 1}}
    ok = __import__("asyncio").run(_process_job(repo, sender, 'w', 'light', job, policy=__import__('app.worker_resource_policy', fromlist=['POLICY']).POLICY, state=_InFlightState()))
    assert ok is True
    assert sender.calls == 0
    assert repo.completed == [13]


def test_video_send_already_sent_skips_send():
    repo = FakeRepo(status='sent')
    sender = FakeSender(repo)
    job = {"id": 14, "job_type": "video_send", "payload_json": {"delivery_id": 1, "rule_id": 1}}
    ok = __import__("asyncio").run(_process_job(repo, sender, 'w', 'heavy', job, policy=__import__('app.worker_resource_policy', fromlist=['POLICY']).POLICY, state=_InFlightState()))
    assert ok is True
    assert sender.calls == 0
    assert repo.completed == [14]


def test_processing_delivery_is_sent_normally():
    repo = FakeRepo(status='processing')
    sender = FakeSender(repo, mode='ok_true')
    job = {"id": 15, "job_type": "repost_single", "payload_json": {"delivery_id": 1, "rule_id": 1}}
    ok = __import__("asyncio").run(_process_job(repo, sender, 'w', 'light', job, policy=__import__('app.worker_resource_policy', fromlist=['POLICY']).POLICY, state=_InFlightState()))
    assert ok is True
    assert sender.calls == 1
    assert repo.completed == [15]
