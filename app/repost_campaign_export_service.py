from __future__ import annotations

import csv
import io
import json


def _clean_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    return str(value).replace("\r\n", "\n").replace("\r", "\n")


def _csv_bytes(headers: list[str], rows: list[list[object]]) -> bytes:
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, delimiter=";", lineterminator="\n")
    writer.writerow(headers)
    for row in rows:
        writer.writerow([_clean_text(x) for x in row])
    return buf.getvalue().encode("utf-8-sig")


def _format_views(item: dict) -> int | None:
    value = item.get("views")
    if value is None:
        value = item.get("views_total")
    if value is None:
        return None
    return int(value)


def _format_message_ids(item: dict) -> str:
    candidates = [item.get("sent_message_ids"), item.get("sent_message_ids_json")]
    for value in candidates:
        if isinstance(value, list):
            return ",".join(str(int(x)) for x in value if x is not None)
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except Exception:
                return _clean_text(value)
            if isinstance(parsed, list):
                return ",".join(str(int(x)) for x in parsed if x is not None)
            return _clean_text(value)
    sent_message_id = item.get("sent_message_id")
    if sent_message_id is not None:
        return str(sent_message_id)
    return ""


def build_campaign_run_report_csv(report: dict) -> bytes:
    headers = [
        "run_id", "saved_post_id", "target_title", "target_id", "target_thread_id", "send_status", "delete_status",
        "sent_message_id", "sent_message_ids", "views", "views_status", "views_source", "sent_at", "delete_after_at",
        "deleted_at", "error_text", "message_url",
    ]
    rows: list[list[object]] = []
    for item in (report or {}).get("items") or []:
        sent_ids_text = _format_message_ids(item)
        rows.append([
            report.get("run_id"),
            report.get("saved_post_id"),
            item.get("target_title"),
            item.get("target_id"),
            item.get("target_thread_id"),
            item.get("send_status"),
            item.get("delete_status"),
            item.get("sent_message_id"),
            sent_ids_text,
            _format_views(item),
            item.get("views_status") or item.get("views_final_status"),
            item.get("views_source") or ("final_snapshot" if item.get("final_snapshot") else None),
            item.get("sent_at"),
            item.get("delete_after_at"),
            item.get("deleted_at"),
            item.get("error_text") or item.get("send_error_text") or item.get("delete_error_text"),
            item.get("message_url"),
        ])
    return _csv_bytes(headers, rows)


def build_campaign_run_report_txt(report: dict) -> str:
    items = (report or {}).get("items") or []
    lines = [
        "📊 Отчёт рекламной кампании",
        "",
        f"Запуск: #{int(report.get('run_id') or 0)}",
        f"Пост: #{int(report.get('saved_post_id') or 0)}",
        f"Всего просмотров: {int(report.get('views_total') or 0):,}".replace(",", " "),
        f"Каналов с данными: {int(report.get('views_available') or 0)} / {len(items)}",
        "",
        "Каналы:",
    ]
    for index, item in enumerate(items, 1):
        title = str(item.get("target_title") or item.get("target_id") or "Канал")
        views = _format_views(item)
        if views is None or str(item.get("views_status") or "").lower() != "ok":
            reason = item.get("error_text") or item.get("send_error_text") or item.get("delete_error_text") or item.get("views_status") or "нет данных"
            lines.append(f"{index}. {title} — нет данных — {reason}")
            continue
        status = str(item.get("delete_status") or "").lower()
        suffix = ""
        if status == "deleted":
            suffix = " — удалено"
        message_ids = _format_message_ids(item)
        message_ids_suffix = f" — сообщения: {message_ids}" if message_ids else ""
        lines.append(f"{index}. {title} — {views:,} просмотров{suffix}{message_ids_suffix}".replace(",", " "))
    return "\n".join(lines)


def build_campaign_post_stats_csv(stats: dict) -> bytes:
    headers = ["saved_post_id", "target_title", "target_id", "views_total", "views_status", "views_source", "runs_count", "unavailable_count", "error_text"]
    rows = []
    for item in (stats or {}).get("channels_stats") or []:
        rows.append([
            stats.get("saved_post_id"),
            item.get("target_title"),
            item.get("target_id"),
            item.get("views_total"),
            item.get("views_status"),
            item.get("views_source"),
            item.get("runs_count"),
            item.get("unavailable_count"),
            item.get("error_text"),
        ])
    return _csv_bytes(headers, rows)


def build_campaign_post_stats_txt(stats: dict) -> str:
    items = (stats or {}).get("channels_stats") or []
    lines = [
        "📄 Статистика рекламного поста",
        "",
        f"Пост: #{int(stats.get('saved_post_id') or 0)}",
        f"Всего просмотров: {int(stats.get('views_total') or 0):,}".replace(",", " "),
        f"Запусков: {int(stats.get('runs_count') or 0)}",
        f"Размещений: {int(stats.get('placements_sent') or 0)}",
        "",
        "Каналы:",
    ]
    for index, item in enumerate(items, 1):
        title = str(item.get("target_title") or item.get("target_id") or "Канал")
        status = str(item.get("views_status") or "").lower()
        if status == "ok":
            lines.append(f"{index}. {title} — {int(item.get('views_total') or 0):,} просмотров".replace(",", " "))
        else:
            lines.append(f"{index}. {title} — нет данных")
    return "\n".join(lines)
