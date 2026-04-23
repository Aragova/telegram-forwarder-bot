from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("forwarder")

JOB_TYPE_REPOST_SINGLE = "repost_single"
JOB_TYPE_REPOST_ALBUM = "repost_album"
JOB_TYPE_VIDEO_DELIVERY = "video_delivery"

JOB_QUEUE_BY_TYPE = {
    JOB_TYPE_REPOST_SINGLE: "light",
    JOB_TYPE_REPOST_ALBUM: "light",
    JOB_TYPE_VIDEO_DELIVERY: "heavy",
}


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
    job_id = repo.create_job(
        job_type=JOB_TYPE_REPOST_SINGLE,
        payload=payload,
        queue=JOB_QUEUE_BY_TYPE[JOB_TYPE_REPOST_SINGLE],
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

    job_id = repo.create_job(
        job_type=JOB_TYPE_REPOST_ALBUM,
        payload=payload,
        queue=JOB_QUEUE_BY_TYPE[JOB_TYPE_REPOST_ALBUM],
    )
    if job_id is not None:
        logger.info(
            "JOB CREATED | Создана задача repost_album для deliveries %s (job #%s)",
            payload["delivery_ids"],
            job_id,
        )
    return job_id


def enqueue_video_delivery(repo, delivery_id: int) -> int | None:
    payload = _delivery_payload(repo, delivery_id)
    if not payload:
        return None

    payload["job_type"] = JOB_TYPE_VIDEO_DELIVERY
    job_id = repo.create_job(
        job_type=JOB_TYPE_VIDEO_DELIVERY,
        payload=payload,
        queue=JOB_QUEUE_BY_TYPE[JOB_TYPE_VIDEO_DELIVERY],
    )
    if job_id is not None:
        logger.info("JOB CREATED | Создана задача video_delivery для delivery #%s (job #%s)", delivery_id, job_id)
    return job_id
