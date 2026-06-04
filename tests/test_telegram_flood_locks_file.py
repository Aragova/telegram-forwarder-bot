import json

import pytest

from app.telegram_flood_locks import TelegramFloodLockStore


def test_lock_is_saved_and_loaded(tmp_path):
    path = tmp_path / "locks.json"
    store = TelegramFloodLockStore(path)

    store.set_lock(
        "7940875697",
        retry_after_seconds=6402,
        method="bot.send_message",
        now_epoch=1000,
    )

    store2 = TelegramFloodLockStore(path)
    locks = store2.load_active_locks(now_epoch=1001)

    assert "7940875697" in locks
    lock = locks["7940875697"]
    assert lock.retry_after_seconds == 6402
    assert lock.last_method == "bot.send_message"
    assert lock.until_epoch == pytest.approx(1000 + 6402 + 3)


def test_get_remaining_seconds(tmp_path):
    path = tmp_path / "locks.json"
    store = TelegramFloodLockStore(path)

    store.set_lock(
        "7940875697",
        retry_after_seconds=100,
        method="bot.send_message",
        now_epoch=1000,
    )

    remaining = store.get_remaining_seconds("7940875697", now_epoch=1050)

    assert remaining == pytest.approx(53)


def test_expired_lock_is_removed(tmp_path):
    path = tmp_path / "locks.json"
    store = TelegramFloodLockStore(path)

    store.set_lock(
        "7940875697",
        retry_after_seconds=10,
        method="bot.send_message",
        now_epoch=1000,
    )

    locks = store.load_active_locks(now_epoch=2000)

    assert locks == {}
    data = json.loads(path.read_text(encoding="utf-8"))
    assert "7940875697" not in data


def test_corrupted_json_does_not_crash(tmp_path):
    path = tmp_path / "locks.json"
    path.write_text("{bad json", encoding="utf-8")
    store = TelegramFloodLockStore(path)

    locks = store.load_active_locks()

    assert locks == {}

    store.set_lock(
        "7940875697",
        retry_after_seconds=10,
        method="bot.send_message",
        now_epoch=1000,
    )
    data = json.loads(path.read_text(encoding="utf-8"))
    assert "7940875697" in data


def test_atomic_write_creates_parent_directory(tmp_path):
    path = tmp_path / "nested" / "data" / "telegram_flood_locks.json"
    store = TelegramFloodLockStore(path)

    store.set_lock(
        "7940875697",
        retry_after_seconds=10,
        method="bot.send_message",
        now_epoch=1000,
    )

    assert path.exists()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["7940875697"]["last_method"] == "bot.send_message"
