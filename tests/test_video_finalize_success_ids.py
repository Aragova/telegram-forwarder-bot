from pathlib import Path

from app.sender import SenderService


class FakeDb:
    def __init__(self):
        self.video_events = []
        self.marked_sent = []

    def log_video_event(self, **kwargs):
        self.video_events.append(kwargs)

    def mark_delivery_sent(self, delivery_id):
        self.marked_sent.append(delivery_id)


def make_service():
    svc = SenderService.__new__(SenderService)
    svc.db = FakeDb()
    return svc


def test_finalize_video_success_logs_candidate_and_valid_ids():
    svc = make_service()

    svc._finalize_video_success_sync(
        delivery_id=10,
        rule_id=20,
        post_id=30,
        source_channel="-100source",
        target_id="-100target",
        target_thread_id=None,
        source_message_id=40,
        sent_message_id=50,
        fallback_mode="deliver_single",
        caption_delivery_mode="auto",
        selected_mode="copy_first",
        caption_requires_premium=False,
        candidate_sent_message_ids=[50, "51", "bad"],
        valid_sent_message_ids=[50],
    )

    assert svc.db.marked_sent == [10]
    event = svc.db.video_events[0]

    assert event["event_type"] == "video_processing_completed"
    assert event["delivery_id"] == 10
    assert event["rule_id"] == 20
    assert event["post_id"] == 30
    assert event["status"] == "sent"

    assert event["extra"]["sent_message_id"] == 50
    assert event["extra"]["candidate_sent_message_ids"] == [50, 51]
    assert event["extra"]["valid_sent_message_ids"] == [50]
    assert event["extra"]["fallback_mode"] == "deliver_single"
    assert event["extra"]["caption_delivery_mode"] == "auto"
    assert event["extra"]["selected_mode"] == "copy_first"
    assert event["extra"]["caption_requires_premium"] is False


def test_finalize_video_success_works_without_optional_ids():
    svc = make_service()

    svc._finalize_video_success_sync(
        delivery_id=10,
        rule_id=20,
        post_id=30,
        source_channel="-100source",
        target_id="-100target",
        target_thread_id=None,
        source_message_id=40,
        sent_message_id=50,
        fallback_mode="deliver_single",
        caption_delivery_mode="auto",
        selected_mode="copy_first",
        caption_requires_premium=False,
    )

    assert svc.db.marked_sent == [10]
    event = svc.db.video_events[0]
    assert event["extra"]["candidate_sent_message_ids"] == []
    assert event["extra"]["valid_sent_message_ids"] == []


def test_video_single_delivery_passes_ids_to_finalize():
    source = Path("app/video_single_delivery.py").read_text(encoding="utf-8")

    assert "candidate_sent_message_ids=sent_message_ids" in source
    assert "valid_sent_message_ids=valid_sent_message_ids" in source
