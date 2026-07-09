from __future__ import annotations

import logging
import time
from pathlib import Path

from .config import settings
from .runtime_utils import run_db
from . import sender_primitives as _sender_primitives

_format_bytes_ru = _sender_primitives._format_bytes_ru
_format_speed_ru = _sender_primitives._format_speed_ru
_format_eta_ru = _sender_primitives._format_eta_ru

logger = logging.getLogger("forwarder")


class SenderFetchDownloadHelpers:
    def __init__(self, owner):
        self.owner = owner

    async def fetch_message(self, source_channel, message_id):
        try:
            entity = int(source_channel) if str(source_channel).lstrip("-").isdigit() else source_channel
            return await self.owner.telethon.get_messages(entity, ids=message_id)
        except Exception as exc:
            logger.warning("Telethon не смог получить сообщение %s/%s: %s", source_channel, message_id, exc); return None

    async def download_video_source(
        self,
        message,
        *,
        delivery_id: int | None = None,
        rule_id: int | None = None,
        post_id: int | None = None,
        source_channel: str | None = None,
        target_id: str | None = None,
        source_message_id: int | None = None,
    ):
        started_at = time.monotonic()
        last_emit_at = 0.0
        last_emit_percent = -1
        last_ui_emit_at = 0.0
        emitted_ui_milestones: set[int] = set()
        ui_milestones = (0, 10, 25, 50, 75, 100)
        ui_min_interval_sec = 20.0

        cache_dir = settings.media_cache_path
        cache_dir.mkdir(parents=True, exist_ok=True)

        message_id = source_message_id or getattr(message, "id", None) or "unknown"
        delivery_part = delivery_id if delivery_id is not None else "manual"
        ext = getattr(getattr(message, "file", None), "ext", None) or ".mp4"
        if not str(ext).startswith("."):
            ext = f".{ext}"
        download_target_path = cache_dir / f"video_src_{delivery_part}_{message_id}_{int(time.time() * 1000)}{ext}"

        def progress_callback(current: int, total: int):
            nonlocal last_emit_at, last_emit_percent, last_ui_emit_at

            now = time.monotonic()
            elapsed = max(now - started_at, 0.001)
            speed = current / elapsed if elapsed > 0 else 0.0
            percent = int((current / total) * 100) if total else 0
            remaining_bytes = max(total - current, 0)
            eta_sec = (remaining_bytes / speed) if speed > 0 else 0.0

            should_emit = False
            if now - last_emit_at >= 1.0:
                should_emit = True
            if percent >= last_emit_percent + 5:
                should_emit = True
            if current == total and total > 0:
                should_emit = True

            if not should_emit:
                return

            last_emit_at = now
            last_emit_percent = percent

            logger.info(
                "📥 Скачивание видео: %s%% | %s из %s | скорость %s | осталось %s",
                percent,
                _format_bytes_ru(current),
                _format_bytes_ru(total),
                _format_speed_ru(speed),
                _format_eta_ru(eta_sec),
            )

            should_emit_ui = False
            for milestone in ui_milestones:
                if percent >= milestone and milestone not in emitted_ui_milestones:
                    emitted_ui_milestones.add(milestone)
                    should_emit_ui = True
                    break
            if now - last_ui_emit_at >= ui_min_interval_sec:
                should_emit_ui = True

            if should_emit_ui and delivery_id is not None and rule_id is not None:
                last_ui_emit_at = now
                try:
                    self.owner._schedule_video_event_log(
                        event_type="video_download_progress",
                        delivery_id=delivery_id,
                        rule_id=rule_id,
                        post_id=post_id,
                        status="processing",
                        extra={
                            "source_channel": source_channel,
                            "target_id": target_id,
                            "source_message_id": source_message_id,
                            "stage": "download",
                            "percent": percent,
                            "downloaded_bytes": current,
                            "total_bytes": total,
                            "speed_bytes_per_sec": round(speed, 2),
                            "eta_sec": int(eta_sec),
                            "downloaded_human": _format_bytes_ru(current),
                            "total_human": _format_bytes_ru(total),
                            "speed_human": _format_speed_ru(speed),
                            "eta_human": _format_eta_ru(eta_sec),
                        },
                    )
                except Exception:
                    pass

        try:
            logger.info("📥 Начинаю скачивание исходного видео...")

            file_path = await self.owner.telethon.download_media(
                message,
                file=str(download_target_path),
                progress_callback=progress_callback,
            )

            if not file_path:
                logger.warning("Не удалось скачать исходное видео: путь не получен")
                return None

            path = Path(file_path)
            if not path.exists() or not path.is_file():
                logger.warning("Не удалось скачать исходное видео: файл не найден после скачивания")
                return None

            file_size = path.stat().st_size
            if file_size <= 0:
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    pass
                logger.warning("Не удалось скачать исходное видео: файл пустой")
                return None

            elapsed_total = time.monotonic() - started_at
            avg_speed = file_size / elapsed_total if elapsed_total > 0 else 0.0

            logger.info(
                "✅ Скачивание завершено: %s за %.1f сек | средняя скорость %s",
                _format_bytes_ru(file_size),
                elapsed_total,
                _format_speed_ru(avg_speed),
            )

            if delivery_id is not None and rule_id is not None:
                try:
                    await run_db(
                        self.owner._log_video_event_sync,
                        event_type="video_download_completed",
                        delivery_id=delivery_id,
                        rule_id=rule_id,
                        post_id=post_id,
                        status="completed",
                        extra={
                            "source_channel": source_channel,
                            "target_id": target_id,
                            "source_message_id": source_message_id,
                            "stage": "download",
                            "file_path": str(path),
                            "downloaded_bytes": file_size,
                            "total_bytes": file_size,
                            "downloaded_human": _format_bytes_ru(file_size),
                            "elapsed_sec": round(elapsed_total, 2),
                            "avg_speed_bytes_per_sec": round(avg_speed, 2),
                            "avg_speed_human": _format_speed_ru(avg_speed),
                        },
                    )
                except Exception:
                    pass

            return path

        except Exception as exc:
            logger.warning("Не удалось скачать исходное видео: %s", exc)

            if delivery_id is not None and rule_id is not None:
                try:
                    await run_db(
                        self.owner._log_video_event_sync,
                        event_type="video_download_failed",
                        delivery_id=delivery_id,
                        rule_id=rule_id,
                        post_id=post_id,
                        status="failed",
                        error_text=str(exc),
                        extra={
                            "source_channel": source_channel,
                            "target_id": target_id,
                            "source_message_id": source_message_id,
                            "stage": "download",
                        },
                    )
                except Exception:
                    pass

            return None
