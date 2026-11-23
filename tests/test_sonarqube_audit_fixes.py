from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.postgres_client import PostgresClient
from app.repost_campaign_report_handlers import _send_export_document


class _Connection:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_postgres_client_rolls_back_base_exception(monkeypatch) -> None:
    client = PostgresClient()
    conn = _Connection()
    monkeypatch.setattr(client, "ensure_driver", lambda: None)
    monkeypatch.setattr(client, "_get_thread_connection", lambda: conn)

    class Interrupted(BaseException):
        pass

    with pytest.raises(Interrupted):
        with client.connect():
            raise Interrupted()

    assert conn.commits == 0
    assert conn.rollbacks == 1

    with client.connect() as result:
        assert result is conn
    assert conn.commits == 1


def test_report_export_is_buffered_without_tempfile() -> None:
    import asyncio
    from aiogram.types import BufferedInputFile

    received = []

    async def send_document_safe(*, chat_id, document):
        received.append((chat_id, document))

    ctx = SimpleNamespace(send_document_safe=send_document_safe)
    callback = SimpleNamespace(message=SimpleNamespace(chat=SimpleNamespace(id=123)))
    asyncio.run(_send_export_document(ctx, callback, filename="audit.csv", content=b"a,b\n1,2\n"))
    assert len(received) == 1
    assert received[0][0] == 123
    assert isinstance(received[0][1], BufferedInputFile)
    assert received[0][1].filename == "audit.csv"
    assert received[0][1].data == b"a,b\n1,2\n"
