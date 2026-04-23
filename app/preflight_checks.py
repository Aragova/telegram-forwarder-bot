from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from app.config import Settings
from app.postgres_client import PostgresClient


class PreflightError(RuntimeError):
    """Критическая ошибка preflight-проверки."""


@dataclass(slots=True)
class PreflightResult:
    ok: bool
    role: str
    messages: list[str]


def _required_env_for_role(role: str) -> tuple[str, ...]:
    normalized = (role or "all").strip().lower()

    base = ("APP_PG_HOST", "APP_PG_PORT", "APP_PG_DB", "APP_PG_USER")
    ui_only = ("BOT_TOKEN", "ADMIN_ID")
    telegram_client = ("API_ID", "API_HASH", "PHONE_NUMBER")

    if normalized == "scheduler":
        return base

    if normalized in {"worker"}:
        return base + telegram_client

    # bot/all/ui
    return base + ui_only + telegram_client


def _check_required_env(role: str) -> list[str]:
    missing = []
    for key in _required_env_for_role(role):
        if not os.getenv(key, "").strip():
            missing.append(key)
    return missing


def _check_dir_writable(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    marker = path / ".preflight_write_test"
    marker.write_text("ok", encoding="utf-8")
    marker.unlink(missing_ok=True)


def run_preflight_checks(
    role: str,
    *,
    settings_obj: Settings,
    pg_client_factory: Callable[[], PostgresClient] = PostgresClient,
) -> PreflightResult:
    messages: list[str] = []
    normalized = (role or "all").strip().lower()

    missing_env = _check_required_env(normalized)
    if missing_env:
        raise PreflightError(
            "Не заполнены обязательные переменные окружения для роли "
            f"{normalized}: {', '.join(missing_env)}"
        )

    if normalized not in {"bot", "ui", "scheduler", "worker", "all"}:
        raise PreflightError(f"Некорректная роль запуска: {normalized}")

    pg_client = pg_client_factory()
    if not pg_client.is_configured():
        raise PreflightError(
            "PostgreSQL не настроен: проверь APP_PG_HOST, APP_PG_PORT, APP_PG_DB, APP_PG_USER"
        )

    if not pg_client.ping():
        raise PreflightError("PostgreSQL недоступен: ping базы завершился ошибкой")
    messages.append("PostgreSQL доступен")

    for target_dir in (settings_obj.media_dir, settings_obj.temp_dir, settings_obj.intros_dir):
        _check_dir_writable(target_dir)
        messages.append(f"Каталог доступен для записи: {target_dir}")

    messages.append(f"Профиль запуска роли: {normalized}")
    return PreflightResult(ok=True, role=normalized, messages=messages)
