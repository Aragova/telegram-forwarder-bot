from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("forwarder")

JOB_TYPE_REPOST_SINGLE = "repost_single"
JOB_TYPE_REPOST_ALBUM = "repost_album"
JOB_TYPE_VIDEO_DELIVERY = "video_delivery"
JOB_TYPE_VIDEO_DOWNLOAD = "video_download"
JOB_TYPE_VIDEO_PROCESS = "video_process"
JOB_TYPE_VIDEO_SEND = "video_send"

JOB_QUEUE_BY_TYPE = {
    JOB_TYPE_REPOST_SINGLE: "light",
    JOB_TYPE_REPOST_ALBUM: "light",
    JOB_TYPE_VIDEO_DELIVERY: "heavy",
    JOB_TYPE_VIDEO_DOWNLOAD: "heavy",
    JOB_TYPE_VIDEO_PROCESS: "heavy",
    JOB_TYPE_VIDEO_SEND: "heavy",
}

JOB_PRIORITY_BY_TYPE = {
    JOB_TYPE_REPOST_ALBUM: 90,
    JOB_TYPE_REPOST_SINGLE: 100,
    # Для heavy video pipeline важен "проток": сначала догоняем process/send уже начатых delivery,
    # и только потом берём новые download.
    JOB_TYPE_VIDEO_SEND: 170,
    JOB_TYPE_VIDEO_PROCESS: 180,
    JOB_TYPE_VIDEO_DOWNLOAD: 200,
    JOB_TYPE_VIDEO_DELIVERY: 200,
}


def build_dedup_key_for_single(delivery_id: int) -> str:
    return f"{JOB_TYPE_REPOST_SINGLE}:delivery:{int(delivery_id)}"


def build_dedup_key_for_video(delivery_id: int) -> str:
    return f"{JOB_TYPE_VIDEO_DOWNLOAD}:delivery:{int(delivery_id)}"


def build_dedup_key_for_video_stage(job_type: str, delivery_id: int) -> str:
    return f"{job_type}:delivery:{int(delivery_id)}"


def build_dedup_key_for_album(rule_id: int, media_group_id: str | None, delivery_ids: list[int]) -> str:
    if media_group_id:
        return f"{JOB_TYPE_REPOST_ALBUM}:rule:{int(rule_id)}:media_group:{media_group_id}"
    ids_key = ",".join(str(int(x)) for x in sorted(int(i) for i in delivery_ids))
    return f"{JOB_TYPE_REPOST_ALBUM}:rule:{int(rule_id)}:deliveries:{ids_key}"


def _delivery_payload(repo, delivery_id: int) -> dict[str, Any] | None:
    row = repo.get_delivery(int(delivery_id))
    if not row:
        return None

    rule = repo.get_rule(int(row["rule_id"]))
    if not rule:
        return None

    return {
        "rule_id": int(row["rule_id"]),
        "delivery_id": int(row["id"]),
        "message_id": int(row["message_id"]),
        "source_channel": str(row["source_channel"]),
        "source_thread_id": row["source_thread_id"],
        "target_id": str(row["target_id"]),
        "target_thread_id": row["target_thread_id"],
        "mode": str(getattr(rule, "mode", "repost") or "repost"),
        "interval": int(getattr(rule, "interval", 0) or 0),
        "schedule_mode": str(getattr(rule, "schedule_mode", "interval") or "interval"),
        "media_group_id": row.get("media_group_id"),
    }


def enqueue_repost_single(repo, delivery_id: int) -> int | None:
    payload = _delivery_payload(repo, delivery_id)
    if not payload:
        return None

    payload["job_type"] = JOB_TYPE_REPOST_SINGLE
    dedup_key = build_dedup_key_for_single(delivery_id)
    job_id = repo.create_job(
        job_type=JOB_TYPE_REPOST_SINGLE,
        payload=payload,
        queue=JOB_QUEUE_BY_TYPE[JOB_TYPE_REPOST_SINGLE],
        priority=JOB_PRIORITY_BY_TYPE[JOB_TYPE_REPOST_SINGLE],
        dedup_key=dedup_key,
    )
    if job_id is not None:
        logger.info("JOB CREATED | Создана задача repost_single для delivery #%s (job #%s)", delivery_id, job_id)
    return job_id


def enqueue_repost_album(repo, delivery_ids: list[int], media_group_id: str | None) -> int | None:
    if not delivery_ids:
        return None

    first_payload = _delivery_payload(repo, int(delivery_ids[0]))
    if not first_payload:
        return None

    payload = {
        **first_payload,
        "job_type": JOB_TYPE_REPOST_ALBUM,
        "delivery_ids": [int(x) for x in delivery_ids],
        "media_group_id": media_group_id,
    }
    dedup_key = build_dedup_key_for_album(
        int(first_payload["rule_id"]),
        media_group_id,
        payload["delivery_ids"],
    )

    job_id = repo.create_job(
        job_type=JOB_TYPE_REPOST_ALBUM,
        payload=payload,
        queue=JOB_QUEUE_BY_TYPE[JOB_TYPE_REPOST_ALBUM],
        priority=JOB_PRIORITY_BY_TYPE[JOB_TYPE_REPOST_ALBUM],
        dedup_key=dedup_key,
    )
    if job_id is not None:
        logger.info(
            "JOB CREATED | Создана задача repost_album для deliveries %s (job #%s)",
            payload["delivery_ids"],
            job_id,
        )
    return job_id


def enqueue_video_delivery(repo, delivery_id: int) -> int | None:
    return enqueue_video_download(repo, delivery_id)


def enqueue_video_delivery_fallback(repo, delivery_id: int) -> int | None:
    payload = _delivery_payload(repo, delivery_id)
    if not payload:
        return None

    payload["job_type"] = JOB_TYPE_VIDEO_DELIVERY
    dedup_key = f"{JOB_TYPE_VIDEO_DELIVERY}:delivery:{int(delivery_id)}"
    job_id = repo.create_job(
        job_type=JOB_TYPE_VIDEO_DELIVERY,
        payload=payload,
        queue=JOB_QUEUE_BY_TYPE[JOB_TYPE_VIDEO_DELIVERY],
        priority=JOB_PRIORITY_BY_TYPE[JOB_TYPE_VIDEO_DELIVERY],
        dedup_key=dedup_key,
    )
    if job_id is not None:
        logger.info("JOB CREATED | Создана fallback-задача video_delivery для delivery #%s (job #%s)", delivery_id, job_id)
    return job_id


VIDEO_ARTIFACT_VERSION = 1
VIDEO_PIPELINE_VERSION = 1


def _build_video_stage_payload(
    payload: dict[str, Any],
    attempt_stage: str,
    *,
    source_video_path: str | None = None,
    processed_video_path: str | None = None,
    thumbnail_path: str | None = None,
    cleanup_paths: list[str] | None = None,
) -> dict[str, Any]:
    next_payload = dict(payload)
    next_payload["job_type"] = payload.get("job_type") or JOB_TYPE_VIDEO_DELIVERY
    next_payload["attempt_stage"] = str(attempt_stage)
    if source_video_path is None:
        source_video_path = payload.get("source_video_path") or payload.get("video_file_path")
    if processed_video_path is None:
        processed_video_path = payload.get("processed_video_path") or payload.get("processed_file_path")
    if thumbnail_path is None:
        thumbnail_path = payload.get("thumbnail_path")
    if cleanup_paths is None:
        cleanup_paths = list(payload.get("cleanup_paths") or [])

    next_payload["source_video_path"] = source_video_path
    next_payload["processed_video_path"] = processed_video_path
    next_payload["thumbnail_path"] = thumbnail_path
    next_payload["cleanup_paths"] = cleanup_paths
    # Backward-compatible aliases.
    next_payload["video_file_path"] = source_video_path
    next_payload["processed_file_path"] = processed_video_path
    next_payload["artifact_version"] = int(payload.get("artifact_version") or VIDEO_ARTIFACT_VERSION)
    next_payload["pipeline_version"] = int(payload.get("pipeline_version") or VIDEO_PIPELINE_VERSION)
    return next_payload


def enqueue_video_download(repo, delivery_id: int) -> int | None:
    payload = _delivery_payload(repo, delivery_id)
    if not payload:
        return None

    payload = _build_video_stage_payload(payload, "download")
    dedup_key = build_dedup_key_for_video_stage(JOB_TYPE_VIDEO_DOWNLOAD, delivery_id)
    job_id = repo.create_job(
        job_type=JOB_TYPE_VIDEO_DOWNLOAD,
        payload=payload,
        queue=JOB_QUEUE_BY_TYPE[JOB_TYPE_VIDEO_DOWNLOAD],
        priority=JOB_PRIORITY_BY_TYPE[JOB_TYPE_VIDEO_DOWNLOAD],
        dedup_key=dedup_key,
    )
    if job_id is not None:
        logger.info("JOB CREATED | Создана задача video_download для delivery #%s (job #%s)", delivery_id, job_id)
    return job_id


def enqueue_video_process(repo, payload: dict[str, Any]) -> int | None:
    delivery_id = int(payload.get("delivery_id") or 0)
    stage_payload = _build_video_stage_payload(
        payload,
        "process",
        source_video_path=payload.get("source_video_path") or payload.get("video_file_path"),
        processed_video_path=payload.get("processed_video_path") or payload.get("processed_file_path"),
        thumbnail_path=payload.get("thumbnail_path"),
        cleanup_paths=list(payload.get("cleanup_paths") or []),
    )
    dedup_key = build_dedup_key_for_video_stage(JOB_TYPE_VIDEO_PROCESS, delivery_id)
    job_id = repo.create_job(
        job_type=JOB_TYPE_VIDEO_PROCESS,
        payload=stage_payload,
        queue=JOB_QUEUE_BY_TYPE[JOB_TYPE_VIDEO_PROCESS],
        priority=JOB_PRIORITY_BY_TYPE[JOB_TYPE_VIDEO_PROCESS],
        dedup_key=dedup_key,
    )
    if job_id is not None:
        logger.info("JOB CREATED | Создана задача video_process для delivery #%s (job #%s)", delivery_id, job_id)
    return job_id


def enqueue_video_send(repo, payload: dict[str, Any]) -> int | None:
    delivery_id = int(payload.get("delivery_id") or 0)
    stage_payload = _build_video_stage_payload(
        payload,
        "send",
        source_video_path=payload.get("source_video_path") or payload.get("video_file_path"),
        processed_video_path=payload.get("processed_video_path") or payload.get("processed_file_path"),
        thumbnail_path=payload.get("thumbnail_path"),
        cleanup_paths=list(payload.get("cleanup_paths") or []),
    )
    dedup_key = build_dedup_key_for_video_stage(JOB_TYPE_VIDEO_SEND, delivery_id)
    job_id = repo.create_job(
        job_type=JOB_TYPE_VIDEO_SEND,
        payload=stage_payload,
        queue=JOB_QUEUE_BY_TYPE[JOB_TYPE_VIDEO_SEND],
        priority=JOB_PRIORITY_BY_TYPE[JOB_TYPE_VIDEO_SEND],
        dedup_key=dedup_key,
    )
    if job_id is not None:
        logger.info("JOB CREATED | Создана задача video_send для delivery #%s (job #%s)", delivery_id, job_id)
    return job_id
