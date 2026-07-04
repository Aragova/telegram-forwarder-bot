from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

from .runtime_utils import run_db
from .sender_primitives import MAX_INVALID_MP4_RETRY

logger = logging.getLogger("forwarder")


class VideoPipelineStages:
    def __init__(self, owner):
        self.owner = owner

    async def execute_download_from_job(
        self,
        *,
        job_id: int | None = None,
        job_attempt: int | None = None,
        rule_id: int,
        delivery_id: int,
        message_id: int,
        source_channel: str,
        target_id: str,
        invalid_file_attempts: int | None = None,
        **_: object,
    ) -> dict:
        owner = self.owner
        logger.info(
            "VIDEO DOWNLOAD START | delivery_id=%s | rule_id=%s | job_id=%s | stage=download",
            delivery_id,
            rule_id,
            job_id,
        )
        message = await owner._fetch_message(source_channel, message_id)
        if not message:
            logger.warning("VIDEO STAGE FAILED | не удалось получить сообщение для delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}

        path = await owner._download_video_source(
            message,
            delivery_id=int(delivery_id),
            rule_id=int(rule_id),
            source_channel=str(source_channel),
            target_id=str(target_id),
            source_message_id=int(message_id),
        )
        if not path:
            logger.warning("VIDEO STAGE FAILED | не удалось скачать видео для delivery_id=%s", delivery_id)
            return {
                "ok": False,
                "fallback_to_legacy": False,
                "retryable": True,
                "error_text": "Не удалось скачать видео",
            }
        probe_ok, probe_error = await owner._validate_mp4_file_for_pipeline(
            Path(path),
            delivery_id=int(delivery_id),
            job_id=job_id,
            stage="download",
        )
        if not probe_ok:
            previous_invalid_attempts = max(0, int(invalid_file_attempts or 0))
            current_job_attempt = max(1, int(job_attempt or 1))
            validation_attempt = previous_invalid_attempts + current_job_attempt
            final_failed = validation_attempt > MAX_INVALID_MP4_RETRY
            compact_error = (probe_error or "битый MP4").replace("битый MP4: ", "", 1)
            current_size = Path(path).stat().st_size if Path(path).is_file() else 0
            if final_failed:
                logger.warning(
                    "VIDEO FILE VALIDATION FINAL FAILED | stage=download | delivery_id=%s | rule_id=%s | job_id=%s | path=%s | size=%s | ffprobe_stderr=%s | action=delivery_faulty_and_job_failed | validation_attempt=%s | max_validation_attempts=%s",
                    delivery_id,
                    rule_id,
                    job_id,
                    path,
                    current_size,
                    compact_error,
                    validation_attempt,
                    MAX_INVALID_MP4_RETRY,
                )
            else:
                logger.warning(
                    "VIDEO FILE VALIDATION FAILED | stage=download | delivery_id=%s | rule_id=%s | job_id=%s | path=%s | size=%s | ffprobe_stderr=%s | action=удалён_файл_и_retry | validation_attempt=%s | max_validation_attempts=%s",
                    delivery_id,
                    rule_id,
                    job_id,
                    path,
                    current_size,
                    compact_error,
                    validation_attempt,
                    MAX_INVALID_MP4_RETRY,
                )
            if final_failed:
                error_text = f"Битый MP4 после повторной загрузки: {compact_error}"
            else:
                error_text = f"Битый MP4: {compact_error}, повторная загрузка 1/{MAX_INVALID_MP4_RETRY}"
            return {
                "ok": False,
                "fallback_to_legacy": False,
                "retryable": not final_failed,
                "error_text": error_text,
                "invalid_source_file": True,
                "final_invalid_source_file": final_failed,
                "ffprobe_stderr": compact_error,
                "validation_attempt": validation_attempt,
                "max_validation_attempts": MAX_INVALID_MP4_RETRY,
                "path": str(path),
                "size": current_size,
            }
        downloaded_size = Path(path).stat().st_size if Path(path).is_file() else 0
        video_info = await owner.video_processor.get_video_info(str(path), use_cache=False)
        logger.info(
            "VIDEO DOWNLOAD DONE | скачивание завершено для delivery_id=%s | path=%s | size=%s | duration=%s",
            delivery_id,
            path,
            downloaded_size,
            round(float(video_info.get("duration") or 0.0), 2) if isinstance(video_info, dict) else None,
        )
        return {"ok": True, "source_video_path": str(path), "fallback_to_legacy": False}

    async def execute_process_from_job(
        self,
        *,
        job_id: int | None = None,
        job_attempt: int | None = None,
        rule_id: int,
        delivery_id: int,
        source_video_path: str | None = None,
        artifact_version: int | None = None,
        invalid_file_attempts: int | None = None,
        **_: object,
    ) -> dict:
        owner = self.owner
        logger.info(
            "VIDEO PROCESS START | delivery_id=%s | rule_id=%s | job_id=%s | stage=process",
            delivery_id,
            rule_id,
            job_id,
        )
        if int(artifact_version or 1) != 1:
            logger.warning("VIDEO FALLBACK TO LEGACY | неподдерживаемая artifact_version=%s | delivery_id=%s", artifact_version, delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}
        if not source_video_path:
            logger.warning("VIDEO STAGE FAILED | отсутствует video_file_path для delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False, "error_text": "Отсутствует путь к исходному файлу"}
        if not Path(source_video_path).is_file():
            logger.warning("VIDEO STAGE FAILED | исходный файл не найден для delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False, "error_text": "Исходный файл не найден"}
        source_path = Path(source_video_path)
        probe_ok, probe_error = await owner._validate_mp4_file_for_pipeline(
            source_path,
            delivery_id=int(delivery_id),
            job_id=job_id,
            stage="process",
        )
        if not probe_ok:
            previous_invalid_attempts = max(0, int(invalid_file_attempts or 0))
            validation_attempt = previous_invalid_attempts + 1
            final_failed = validation_attempt > MAX_INVALID_MP4_RETRY
            compact_error = (probe_error or "битый MP4").replace("битый MP4: ", "", 1)
            current_size = source_path.stat().st_size if source_path.is_file() else 0
            if final_failed:
                logger.warning(
                    "VIDEO FILE VALIDATION FINAL FAILED | stage=process | delivery_id=%s | rule_id=%s | job_id=%s | path=%s | size=%s | ffprobe_stderr=%s | action=delivery_faulty_and_job_failed | validation_attempt=%s | max_validation_attempts=%s",
                    delivery_id,
                    rule_id,
                    job_id,
                    source_path,
                    current_size,
                    compact_error,
                    validation_attempt,
                    MAX_INVALID_MP4_RETRY,
                )
            else:
                logger.warning(
                    "VIDEO FILE VALIDATION FAILED | stage=process | delivery_id=%s | rule_id=%s | job_id=%s | path=%s | size=%s | ffprobe_stderr=%s | action=удалён_файл_и_retry | validation_attempt=%s | max_validation_attempts=%s",
                    delivery_id,
                    rule_id,
                    job_id,
                    source_path,
                    current_size,
                    compact_error,
                    validation_attempt,
                    MAX_INVALID_MP4_RETRY,
                )
            if final_failed:
                error_text = f"Битый MP4 после повторной загрузки: {compact_error}"
            else:
                error_text = f"Битый MP4: {compact_error}, повторная загрузка 1/{MAX_INVALID_MP4_RETRY}"
            return {
                "ok": False,
                "fallback_to_legacy": False,
                "retryable": False,
                "restart_download": not final_failed,
                "error_text": error_text,
                "invalid_source_file": True,
                "final_invalid_source_file": final_failed,
                "ffprobe_stderr": compact_error,
                "validation_attempt": validation_attempt,
                "max_validation_attempts": MAX_INVALID_MP4_RETRY,
                "invalid_file_attempts": validation_attempt,
                "path": str(source_path),
                "size": current_size,
            }

        rule = await run_db(owner.db.get_rule, int(rule_id))
        if not rule:
            logger.warning("VIDEO FALLBACK TO LEGACY | правило не найдено для process | delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}

        clip_duration_seconds = int(getattr(rule, "video_clip_duration_seconds", None) or 118)
        logger.info(
            "VIDEO_PROCESS_CLIP_DURATION | rule_id=%s | delivery_id=%s | clip_duration_seconds=%s",
            rule_id,
            delivery_id,
            clip_duration_seconds,
        )

        horizontal_intro, vertical_intro = await run_db(owner._get_rule_intro_items_sync, rule)
        processed_result = await owner.video_processor.build_processed_video(
            input_file_path=str(source_video_path),
            add_intro=bool(getattr(rule, "video_add_intro", False)),
            intro_name_horizontal=getattr(horizontal_intro, "file_name", None) if horizontal_intro else None,
            intro_name_vertical=getattr(vertical_intro, "file_name", None) if vertical_intro else None,
            intro_item_horizontal=horizontal_intro,
            intro_item_vertical=vertical_intro,
            clip_duration_seconds=clip_duration_seconds,
        )
        if not processed_result:
            logger.warning("VIDEO STAGE FAILED | VideoProcessor не смог обработать файл для delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": False, "retryable": True}
        logger.info(
            "VIDEO PROCESS DONE | обработка стадии завершена для delivery_id=%s | source=%s | processed=%s",
            delivery_id,
            source_video_path,
            processed_result.get("processed_video_path"),
        )
        return {"ok": True, "fallback_to_legacy": False, **processed_result}

    async def validate_mp4_file_for_pipeline(
        self,
        file_path: Path,
        *,
        delivery_id: int,
        job_id: int | None,
        stage: str,
    ) -> tuple[bool, str | None]:
        if not file_path.exists() or not file_path.is_file():
            error_text = "Файл для проверки не найден"
            logger.warning(
                "VIDEO FILE VALIDATION FAILED | stage=%s | delivery_id=%s | job_id=%s | path=%s | size=%s | ffprobe_stderr=%s | action=retry",
                stage,
                delivery_id,
                job_id,
                str(file_path),
                0,
                error_text,
            )
            return False, error_text

        file_size = file_path.stat().st_size
        probe_cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_format",
            "-show_streams",
            "-print_format",
            "json",
            str(file_path),
        ]
        process = await asyncio.create_subprocess_exec(
            *probe_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        stderr_text = stderr.decode("utf-8", errors="ignore").strip()
        stdout_text = stdout.decode("utf-8", errors="ignore").strip()

        probe_payload = {}
        if stdout_text:
            try:
                probe_payload = json.loads(stdout_text)
            except Exception:
                probe_payload = {}

        streams = probe_payload.get("streams") if isinstance(probe_payload, dict) else None
        format_info = probe_payload.get("format") if isinstance(probe_payload, dict) else None
        empty_probe_data = (not isinstance(streams, list) or not streams) or (not isinstance(format_info, dict) or not format_info)

        if process.returncode == 0 and not empty_probe_data:
            return True, None

        compact_stderr = (stderr_text or "").replace("\n", " | ").strip()
        lower_stderr = compact_stderr.lower()
        if "moov atom not found" in lower_stderr:
            reason = "битый MP4: moov atom not found"
        elif "invalid data found when processing input" in lower_stderr:
            reason = "битый MP4: Invalid data found when processing input"
        elif empty_probe_data:
            reason = "битый MP4: ffprobe вернул пустые stream/format данные"
        else:
            fallback = compact_stderr or "ffprobe завершился с ошибкой"
            reason = f"битый MP4: {fallback[:500]}"

        removed = False
        try:
            file_path.unlink(missing_ok=True)
            removed = True
        except Exception as cleanup_exc:
            logger.warning(
                "VIDEO FILE VALIDATION CLEANUP FAILED | stage=%s | delivery_id=%s | job_id=%s | path=%s | error=%s",
                stage,
                delivery_id,
                job_id,
                str(file_path),
                cleanup_exc,
            )

        action = "удалён_файл_и_retry" if removed else "retry_без_удаления"
        logger.warning(
            "VIDEO FILE VALIDATION FAILED | stage=%s | delivery_id=%s | job_id=%s | path=%s | size=%s | ffprobe_stderr=%s | action=%s",
            stage,
            delivery_id,
            job_id,
            str(file_path),
            file_size,
            compact_stderr,
            action,
        )
        return False, reason

