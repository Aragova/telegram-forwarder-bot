from app.delivery_error_text import describe_delivery_error

import pytest


@pytest.mark.parametrize("code", [
    "album_member_set_changed_manual_review_no_resend",
    "album_conflicting_member_vector_manual_review_no_resend",
    "album_cache_member_provenance_unavailable_no_resend",
    "album_membership_changed_pre_send_manual_review",
])
def test_rescan_instructions_for_known_queue_conflicts(code):
    text = describe_delivery_error(code).text()
    assert "Пересканировать" in text and "Начать заново" in text
    assert "Начать с этого" in text and "возможен дубль" in text
    assert code not in text


@pytest.mark.parametrize("code", [
    "copy_single_uncertain_no_fallback: <secret>",
    "copy_album_uncertain_no_fallback",
    "telethon_send_attempted_result_unknown_non_retryable",
    "video_send_accepted_target_id_unresolved_non_retryable",
    "self_loop_album_accepted_unverified_no_resend",
])
def test_uncertain_send_never_recommends_blind_rescan(code):
    text = describe_delivery_error(code).text()
    assert "Проверьте канал-получатель вручную" in text
    assert "Пересканировать" not in text
    assert "<secret>" not in text


@pytest.mark.parametrize("raw,expected", [
    ("Request Entity Too Large", "Файл слишком большой"),
    ("TelegramRetryAfter", "Ограничение Telegram"),
    ("TelegramForbiddenError", "Нет доступа к чату"),
    ("Bad Request: chat not found", "Чат или сообщение не найдено"),
    ("DocumentInvalidError", "Некорректный медиафайл"),
    ("Битый MP4: пустой файл", "Некорректный медиафайл"),
    ("Не удалось скачать видео", "Не удалось скачать файл"),
    ("Не удалось скачать элемент альбома 2/3", "Не удалось скачать файл"),
    ("Исходный файл не найден", "Файл на сервере не найден"),
    ("Ошибка при склейке видео", "Ошибка обработки видео"),
    ("Недостаточно свободного места на диске", "Недостаточно места на сервере"),
    ("telegram_video_processing_pending", "Telegram обрабатывает видео"),
    ("invalid media group size", "Некорректный состав альбома"),
    ("verify_album_count_mismatch", "Отправка не подтверждена"),
    ("telethon_send_file_returned_empty", "Отправка не подтверждена"),
    ("TimeoutError", "Превышено время ожидания"),
    ("No space left on device", "Недостаточно места на сервере"),
    ("self_target_echo", "Защита от повторного захвата"),
    ("caption_or_edit", "Пропуск по настройкам"),
    ("rule_rescan_cancelled", "Отменено пересканированием"),
    ("unexpected <b>SECRET_TOKEN</b>", "Не удалось определить причину"),
    (None, "Причина не записана"),
])
def test_known_and_unknown_errors_are_safe_russian(raw, expected):
    description = describe_delivery_error(raw)
    assert description.title == expected
    assert "Что делать:" in description.text()
    assert "SECRET_TOKEN" not in description.text()
    assert "<b>" not in description.text()


def test_error_type_used_when_raw_text_is_unknown():
    assert describe_delivery_error("unrecognised details", "TelegramRetryAfter").title == "Ограничение Telegram"
