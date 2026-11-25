"""Durable crash-safe buffer for Telegram scheduled-publication mappings.

This is a local write-ahead queue, NOT a second source of truth. Each
mapping is deleted only after PostgreSQL confirms it was recorded.
The file must live next to the persistent Telethon session.
"""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path


class PublicationMappingSpool:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path), timeout=10, isolation_level="DEFERRED")
        try:
            conn.execute("PRAGMA busy_timeout=10000")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute(
                """CREATE TABLE IF NOT EXISTS pending_publication_mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    peer_id INTEGER NOT NULL,
                    scheduled_json TEXT NOT NULL,
                    published_json TEXT NOT NULL
                )"""
            )
            conn.commit()
            return conn
        except BaseException:
            conn.close()
            raise

    def append(self, peer: int, scheduled: list[int], published: list[int]) -> None:
        if len(scheduled) != len(published) or not scheduled:
            raise ValueError("Expected complete publication mapping pairs")
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """INSERT INTO pending_publication_mappings(
                       peer_id, scheduled_json, published_json
                   ) VALUES (?, ?, ?)""",
                (
                    int(peer),
                    json.dumps([int(value) for value in scheduled]),
                    json.dumps([int(value) for value in published]),
                ),
            )

    def pending(self, limit: int = 128) -> list[tuple[int, int, list[int], list[int]]]:
        with closing(self._connect()) as conn, conn:
            rows = conn.execute(
                """SELECT id,peer_id,scheduled_json,published_json
                   FROM pending_publication_mappings ORDER BY id LIMIT ?""",
                (max(1, int(limit)),),
            ).fetchall()
        return [
            (int(row_id), int(peer), json.loads(old), json.loads(new))
            for row_id, peer, old, new in rows
        ]

    def acknowledge(self, row_id: int) -> None:
        with closing(self._connect()) as conn, conn:
            conn.execute(
                "DELETE FROM pending_publication_mappings WHERE id=?",
                (int(row_id),),
            )



class PublicationFallbackJournal:
    """Fsynced fallback for publication mappings when SQLite itself fails.

    Every record is replayed idempotently into PostgreSQL. The journal may
    be cleared only after all its records have been committed there.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def append(self, peer: int, scheduled: list[int], published: list[int]) -> None:
        if not scheduled or len(scheduled) != len(published):
            raise ValueError("Expected complete fallback publication pairs")
        record = {
            "peer": int(peer),
            "scheduled": [int(value) for value in scheduled],
            "published": [int(value) for value in published],
        }
        data = (json.dumps(record, separators=(",", ":")) + "\n").encode("utf-8")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, "ab") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())

    def pending(self) -> list[tuple[int, list[int], list[int]]]:
        if not self.path.exists():
            return []
        records = []
        with self.path.open("rb") as stream:
            for line in stream:
                if not line.endswith(b"\n"):
                    raise RuntimeError("Incomplete publication fallback journal; operator review required")
                try:
                    raw = json.loads(line)
                    peer = raw["peer"]
                    old = raw["scheduled"]
                    new = raw["published"]
                    if (type(peer) is not int or peer == 0
                            or not isinstance(old, list) or not old
                            or not isinstance(new, list) or len(old) != len(new)
                            or any(type(value) is not int or value <= 0 for value in old + new)):
                        raise ValueError("Invalid publication journal pair")
                    records.append((peer, old, new))
                except (ValueError, KeyError, TypeError) as exc:
                    raise RuntimeError("Corrupt publication fallback journal; operator review required") from exc
        return records

    def clear(self) -> None:
        # Called only under the publication journal lock after PG confirms all
        # entries. Atomic replacement retains the old bytes on interruption.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".clearing")
        with tmp.open("wb") as stream:
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, self.path)
        dirfd = os.open(str(self.path.parent), os.O_RDONLY)
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)
