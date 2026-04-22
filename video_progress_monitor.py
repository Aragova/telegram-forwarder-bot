from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta

from app.db import Database
from app.config import settings


USER_TZ = timezone(timedelta(hours=3))


def to_local_time(dt_str: str | None) -> str:
    if not dt_str:
        return datetime.now(USER_TZ).strftime("%d.%m.%Y %H:%M:%S")
    try:
        dt = datetime.fromisoformat(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(USER_TZ).strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        return str(dt_str)


def parse_extra(extra_raw) -> dict:
    if not extra_raw:
        return {}
    if isinstance(extra_raw, dict):
        return extra_raw
    try:
        return json.loads(extra_raw)
    except Exception:
        return {}


def safe(row, key, default=None):
    try:
        return row[key]
    except Exception:
        return default


def short(text: str | None, limit: int = 160) -> str:
    if not text:
        return ""
    text = str(text).replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def format_prefix(row) -> str:
    created_at = to_local_time(safe(row, "created_at"))
    delivery_id = safe(row, "delivery_id")
    rule_id = safe(row, "rule_id")
    post_id = safe(row, "post_id")

    parts = [f"[{created_at}]"]

    if delivery_id is not None:
        parts.append(f"доставка #{delivery_id}")
    if rule_id is not None:
        parts.append(f"правило #{rule_id}")
    if post_id is not None:
        parts.append(f"пост #{post_id}")

    return " | ".join(parts)


def stage_name_ru(stage: str | None) -> str:
    mapping = {
        "pipeline": "общий процесс",
        "download": "скачивание",
        "probe": "анализ видео",
        "trim": "обрезка",
        "normalize": "нормализация",
        "intro": "подготовка заставки",
        "concat": "склейка",
        "thumbnail": "создание превью",
        "send": "отправка",
    }
    return mapping.get(stage or "", stage or "неизвестный этап")


def build_human_line(row) -> str:
    prefix = format_prefix(row)
    event_type = safe(row, "event_type", "") or ""
    status = safe(row, "status", "") or ""
    error_text = safe(row, "error_text")
    extra = parse_extra(safe(row, "extra_json"))

    stage = extra.get("stage")
    stage_ru = stage_name_ru(stage)

    # =========================================
    # СКАЧИВАНИЕ ИСХОДНОГО ВИДЕО
    # =========================================
    if event_type == "video_download_started":
        media_kind = extra.get("media_kind")
        trim_seconds = extra.get("trim_seconds")
        details = []
        if media_kind:
            details.append(f"тип медиа: {media_kind}")
        if trim_seconds:
            details.append(f"лимит обрезки: {trim_seconds} сек")

        tail = f" ({', '.join(details)})" if details else ""
        return f"{prefix} | ▶️ Подготовка видеодоставки начата{tail}"

    if event_type == "video_download_progress":
        percent = extra.get("percent")
        downloaded_human = extra.get("downloaded_human")
        total_human = extra.get("total_human")
        speed_human = extra.get("speed_human")
        eta_human = extra.get("eta_human")

        parts = [f"{prefix}", "📥 Скачивание видео"]

        if percent is not None:
            parts.append(f"{int(percent)}%")
        if downloaded_human and total_human:
            parts.append(f"{downloaded_human} из {total_human}")
        if speed_human:
            parts.append(f"скорость {speed_human}")
        if eta_human:
            parts.append(f"осталось {eta_human}")

        return " | ".join(parts)

    if event_type == "video_download_completed":
        downloaded_human = extra.get("downloaded_human")
        elapsed_sec = extra.get("elapsed_sec")
        avg_speed_human = extra.get("avg_speed_human")

        parts = [f"{prefix}", "✅ Скачивание видео завершено"]
        if downloaded_human:
            parts.append(downloaded_human)
        if elapsed_sec is not None:
            parts.append(f"за {float(elapsed_sec):.1f} сек")
        if avg_speed_human:
            parts.append(f"средняя скорость {avg_speed_human}")

        return " | ".join(parts)

    if event_type == "video_download_failed":
        return f"{prefix} | ❌ Ошибка скачивания видео: {short(error_text or 'неизвестная ошибка')}"

    # =========================================
    # ЭТАПЫ PIPELINE
    # =========================================
    if event_type == "video_stage_started":
        return f"{prefix} | ▶️ Начат этап: {stage_ru}"

    if event_type == "video_stage_completed":
        if stage == "download":
            file_size_mb = extra.get("file_size_mb")
            if file_size_mb is not None:
                return f"{prefix} | ✅ Скачивание завершено: {float(file_size_mb):.1f} МБ"

        if stage == "concat" and extra.get("mode") == "copy_without_concat":
            return f"{prefix} | ✅ Этап завершён: {stage_ru} (склейка не понадобилась)"

        return f"{prefix} | ✅ Этап завершён: {stage_ru}"

    if event_type == "video_stage_failed":
        if error_text:
            return f"{prefix} | ❌ Ошибка на этапе «{stage_ru}»: {short(error_text)}"
        return f"{prefix} | ❌ Ошибка на этапе «{stage_ru}»"

    if event_type == "video_ffmpeg_progress":
        operation = extra.get("operation")
        percent = extra.get("percent")
        processed_sec = extra.get("processed_sec")
        total_sec = extra.get("total_sec")
        speed = extra.get("speed")

        parts = [f"{prefix}", "🎬 Обработка видео"]

        if operation:
            parts.append(str(operation))
        elif stage:
            parts.append(stage_ru)

        if percent is not None:
            parts.append(f"{float(percent):.1f}%")
        if processed_sec is not None and total_sec is not None:
            parts.append(f"{float(processed_sec):.1f} / {float(total_sec):.1f} сек")
        if speed:
            parts.append(f"скорость {speed}")

        return " | ".join(parts)

    if event_type == "video_send_retry":
        attempt = extra.get("attempt")
        max_retries = extra.get("max_retries")
        if attempt is not None and max_retries is not None:
            return f"{prefix} | 🔁 Повторная попытка отправки: {attempt} из {max_retries}"
        if attempt is not None:
            return f"{prefix} | 🔁 Повторная попытка отправки: {attempt}"
        return f"{prefix} | 🔁 Повторная попытка отправки"

    # =========================================
    # ОБЩИЕ СОБЫТИЯ ВИДЕОРЕЖИМА
    # =========================================
    if event_type == "video_processing_started":
        return f"{prefix} | 🚀 Запущена обработка видеодоставки"

    if event_type == "video_processing_completed":
        media_kind = extra.get("media_kind")
        skipped = extra.get("skipped")
        skip_reason = extra.get("skip_reason")

        if skipped:
            reason_tail = f", причина: {skip_reason}" if skip_reason else ""
            kind_tail = f", тип: {media_kind}" if media_kind else ""
            return f"{prefix} | ✅ Видеорежим завершён без обработки{kind_tail}{reason_tail}"

        return f"{prefix} | ✅ Видеодоставка успешно завершена"

    if event_type == "video_processing_failed":
        return f"{prefix} | ❌ Ошибка видеодоставки: {short(error_text or 'неизвестная ошибка')}"

    # =========================================
    # ОБЫЧНЫЕ СОБЫТИЯ ДОСТАВКИ
    # =========================================
    if event_type == "delivery_started":
        source_channel = extra.get("source_channel")
        target_id = extra.get("target_id")
        message_id = extra.get("message_id")
        return (
            f"{prefix} | ▶️ Начата доставка сообщения"
            f" | источник {source_channel} | сообщение {message_id} | получатель {target_id}"
        )

    if event_type == "delivery_sent":
        method = extra.get("method")
        sent_message_id = extra.get("sent_message_id")
        parts = [f"{prefix}", "✅ Сообщение доставлено"]
        if method:
            parts.append(f"способ: {method}")
        if sent_message_id:
            parts.append(f"id отправленного сообщения: {sent_message_id}")
        return " | ".join(parts)

    if event_type == "delivery_failed":
        return f"{prefix} | ❌ Ошибка доставки: {short(error_text or 'неизвестная ошибка')}"

    if event_type == "delivery_process_exception":
        return f"{prefix} | ❌ Критическая ошибка доставки: {short(error_text or 'неизвестная ошибка')}"

    # =========================================
    # ПРОЧЕЕ
    # =========================================
    if error_text:
        return f"{prefix} | {event_type} | {status} | ошибка: {short(error_text)}"

    return f"{prefix} | {event_type} | {status}"

def print_header():
    print("=" * 110)
    print("ЖИВОЙ МОНИТОР ВИДЕО И ДОСТАВОК")
    print(f"База данных: {settings.db_path}")
    print(f"Запуск: {datetime.now(USER_TZ).strftime('%d.%m.%Y %H:%M:%S')}")
    print("Новые события появляются внизу. Экран не мигает и не очищается.")
    print("=" * 110)


def main():
    db = Database()

    print_header()

    last_seen_id = 0
    idle_counter = 0

    while True:
        try:
            rows = list(db.get_recent_video_audit(limit=300))

            if rows:
                # На старте просто запоминаем последний id, чтобы не вываливать старый хвост
                if last_seen_id == 0:
                    try:
                        last_seen_id = max(int(safe(row, "id", 0) or 0) for row in rows)
                    except Exception:
                        last_seen_id = 0
                    print("Монитор подключён. Жду новые события...\n")
                else:
                    new_rows = []
                    for row in reversed(rows):
                        row_id = int(safe(row, "id", 0) or 0)
                        if row_id > last_seen_id:
                            new_rows.append(row)

                    if new_rows:
                        for row in new_rows:
                            print(build_human_line(row))
                            row_id = int(safe(row, "id", 0) or 0)
                            if row_id > last_seen_id:
                                last_seen_id = row_id
                        idle_counter = 0
                    else:
                        idle_counter += 1
            else:
                if last_seen_id == 0:
                    print("В audit_log пока нет событий.\n")
                    last_seen_id = -1

            time.sleep(1)

        except KeyboardInterrupt:
            print("\nОстановлено вручную.")
            break
        except Exception as e:
            print(f"[{datetime.now(USER_TZ).strftime('%d.%m.%Y %H:%M:%S')}] ❌ Ошибка монитора: {e}")
            time.sleep(2)


if __name__ == "__main__":
    main()
