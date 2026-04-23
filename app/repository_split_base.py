from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta
from typing import Any


class RepositorySplitBase:
    def __init__(self, root_repo: Any) -> None:
        self._root = root_repo

    def connect(self):
        return self._root.connect()

    def fetch_inserted_id(self, conn, sql: str, params: tuple[Any, ...]) -> int | None:
        return self._root._fetch_inserted_id(conn, sql, params)

    def ensure_tenant_for_admin_conn(self, conn, admin_id: int) -> int:
        return self._root._ensure_tenant_for_admin_conn(conn, admin_id)

    @staticmethod
    def json_dumps(value: Any) -> str | None:
        if value is None:
            return None

        def _default(obj):
            from dataclasses import asdict, is_dataclass

            if isinstance(obj, (datetime, date, time)):
                return obj.isoformat()
            if isinstance(obj, timedelta):
                return obj.total_seconds()
            if is_dataclass(obj):
                return asdict(obj)
            return str(obj)

        return json.dumps(value, ensure_ascii=False, default=_default)

    @staticmethod
    def safe_json_loads(raw: Any, default: Any) -> Any:
        if raw is None:
            return default
        if isinstance(raw, (dict, list)):
            return raw
        if isinstance(raw, (bytes, bytearray)):
            try:
                raw = raw.decode("utf-8", errors="ignore")
            except Exception:
                return default
        if isinstance(raw, str):
            raw = raw.strip()
            if not raw:
                return default
            try:
                return json.loads(raw)
            except Exception:
                return default
        return default
