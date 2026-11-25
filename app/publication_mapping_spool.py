"""Durable crash-safe buffer for Telegram scheduled-publication mappings.

This is a local write-ahead queue, NOT a second source of truth. Each
mapping is deleted only after PostgreSQL confirms it was recorded.
The file must live next to the persistent Telethon session.
"""
from __future__ import annotations

import json
import sqlite3
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
        with self._connect() as conn:
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
        with self._connect() as conn:
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
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM pending_publication_mappings WHERE id=?",
                (int(row_id),),
            )
