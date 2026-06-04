from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger("forwarder.ui")

DEFAULT_TELEGRAM_FLOOD_LOCKS_PATH = Path("data/telegram_flood_locks.json")


@dataclass(slots=True)
class TelegramFloodLock:
    chat_id: str
    until_epoch: float
    last_method: str
    retry_after_seconds: int
    updated_at: str


class TelegramFloodLockStore:
    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path is not None else DEFAULT_TELEGRAM_FLOOD_LOCKS_PATH

    def load_active_locks(
        self, *, now_epoch: float | None = None
    ) -> dict[str, TelegramFloodLock]:
        now = time.time() if now_epoch is None else float(now_epoch)
        raw_locks = self._read_raw_locks()
        if not raw_locks:
            return {}

        active_locks: dict[str, TelegramFloodLock] = {}
        expired_count = 0
        for chat_id, payload in raw_locks.items():
            lock = self._lock_from_payload(chat_id, payload)
            if lock is None:
                expired_count += 1
                continue
            if lock.until_epoch <= now:
                expired_count += 1
                continue
            active_locks[lock.chat_id] = lock

        if expired_count > 0:
            if self._write_raw_locks(self._locks_to_raw(active_locks)):
                logger.info(
                    "UI_FLOOD_LOCKS | EXPIRED_CLEANUP | count=%s", expired_count
                )

        logger.info("UI_FLOOD_LOCKS | LOADED | count=%s", len(active_locks))
        return active_locks

    def get_remaining_seconds(
        self, chat_id: int | str, *, now_epoch: float | None = None
    ) -> float:
        key = str(chat_id)
        now = time.time() if now_epoch is None else float(now_epoch)
        locks = self.load_active_locks(now_epoch=now)
        lock = locks.get(key)
        if lock is None:
            return 0.0
        return max(0.0, lock.until_epoch - now)

    def set_lock(
        self,
        chat_id: int | str,
        *,
        retry_after_seconds: int | float,
        method: str,
        now_epoch: float | None = None,
        safety_seconds: int | float = 3,
    ) -> TelegramFloodLock:
        now = time.time() if now_epoch is None else float(now_epoch)
        retry_after_value = max(0.0, float(retry_after_seconds))
        safety_value = max(0.0, float(safety_seconds))
        retry_after_int = int(retry_after_value)
        key = str(chat_id)
        lock = TelegramFloodLock(
            chat_id=key,
            until_epoch=now + retry_after_value + safety_value,
            last_method=method,
            retry_after_seconds=retry_after_int,
            updated_at=datetime.fromtimestamp(now, tz=UTC).isoformat(),
        )

        locks = self.load_active_locks(now_epoch=now)
        locks[key] = lock
        if self._write_raw_locks(self._locks_to_raw(locks)):
            logger.info(
                "UI_FLOOD_LOCKS | SAVED | chat_id=%s | method=%s | retry_after=%s | until_epoch=%s",
                key,
                method,
                retry_after_int,
                lock.until_epoch,
            )
        return lock

    def clear_expired(self, *, now_epoch: float | None = None) -> int:
        now = time.time() if now_epoch is None else float(now_epoch)
        raw_locks = self._read_raw_locks()
        if not raw_locks:
            return 0

        active_locks: dict[str, TelegramFloodLock] = {}
        expired_count = 0
        for chat_id, payload in raw_locks.items():
            lock = self._lock_from_payload(chat_id, payload)
            if lock is None or lock.until_epoch <= now:
                expired_count += 1
                continue
            active_locks[lock.chat_id] = lock

        if expired_count > 0:
            if self._write_raw_locks(self._locks_to_raw(active_locks)):
                logger.info(
                    "UI_FLOOD_LOCKS | EXPIRED_CLEANUP | count=%s", expired_count
                )
        return expired_count

    def clear_lock(self, chat_id: int | str) -> bool:
        key = str(chat_id)
        raw_locks = self._read_raw_locks()
        if key not in raw_locks:
            return False
        raw_locks.pop(key, None)
        self._write_raw_locks(raw_locks)
        return True

    def _read_raw_locks(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}

        try:
            with self.path.open("r", encoding="utf-8") as file:
                data = json.load(file)
        except json.JSONDecodeError as exc:
            self._log_storage_warning(exc)
            self._mark_corrupt_file()
            return {}
        except OSError as exc:
            self._log_storage_warning(exc)
            return {}

        if not isinstance(data, dict):
            self._log_storage_warning(ValueError("locks JSON root is not an object"))
            return {}

        result: dict[str, dict[str, Any]] = {}
        for chat_id, payload in data.items():
            if isinstance(payload, dict):
                result[str(chat_id)] = payload
        return result

    def _write_raw_locks(self, raw_locks: dict[str, dict[str, Any]]) -> bool:
        tmp_path = self.path.with_name(f"{self.path.name}.tmp")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with tmp_path.open("w", encoding="utf-8") as file:
                json.dump(
                    raw_locks,
                    file,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
                file.write("\n")
                file.flush()
                os.fsync(file.fileno())
            os.replace(tmp_path, self.path)
            return True
        except OSError as exc:
            self._log_storage_warning(exc)
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError as cleanup_exc:
                self._log_storage_warning(cleanup_exc)
            return False

    def _lock_from_payload(
        self, chat_id: str, payload: dict[str, Any]
    ) -> TelegramFloodLock | None:
        try:
            return TelegramFloodLock(
                chat_id=str(chat_id),
                until_epoch=float(payload["until_epoch"]),
                last_method=str(payload.get("last_method") or ""),
                retry_after_seconds=int(payload.get("retry_after_seconds") or 0),
                updated_at=str(payload.get("updated_at") or ""),
            )
        except (TypeError, ValueError, KeyError) as exc:
            self._log_storage_warning(exc)
            return None

    def _locks_to_raw(
        self, locks: dict[str, TelegramFloodLock]
    ) -> dict[str, dict[str, Any]]:
        raw_locks: dict[str, dict[str, Any]] = {}
        for chat_id, lock in locks.items():
            payload = asdict(lock)
            payload.pop("chat_id", None)
            payload["until_monotonic_unusable"] = False
            raw_locks[str(chat_id)] = payload
        return raw_locks

    def _mark_corrupt_file(self) -> None:
        corrupt_path = self.path.with_suffix(f"{self.path.suffix}.corrupt")
        try:
            os.replace(self.path, corrupt_path)
        except OSError as exc:
            self._log_storage_warning(exc)

    def _log_storage_warning(self, exc: Exception) -> None:
        logger.warning(
            "UI_FLOOD_LOCKS | STORAGE_WARNING | path=%s | error=%s",
            self.path,
            exc,
        )
