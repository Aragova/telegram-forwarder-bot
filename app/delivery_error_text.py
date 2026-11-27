"""User-facing delivery errors. Raw codes remain unchanged in storage and logs."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DeliveryError:
    title: str
    reason: str
    action: str

    def text(self) -> str:
        return f"{self.reason} Что делать: {self.action}"


_RESCAN = DeliveryError(
    "Несоответствие очереди и истории отправки",
    "Состав альбома не совпадает с ранее сохранёнными данными отправки. "
    "Повторная отправка заблокирована для защиты от дублей.",
    "Проверьте, до какого поста канал уже опубликован. В правиле выберите "
    "«Пересканировать» → «Начать заново», затем «Начать с этого» на нужном посте. "
    "При выборе уже опубликованного поста возможен дубль.",
)
_UNCERTAIN = DeliveryError(
    "Отправка не подтверждена",
    "Telegram мог принять сообщение, но бот не смог подтвердить его публикацию.",
    "Проверьте канал-получатель вручную. Не повторяйте отправку вслепую: возможен дубль.",
)
_RATE_LIMIT = DeliveryError(
    "Ограничение Telegram",
    "Telegram временно ограничил частоту запросов.",
    "Дождитесь окончания ограничения; не запускайте повторные отправки вручную.",
)
_ECHO = DeliveryError(
    "Защита от повторного захвата",
    "Сообщение похоже на публикацию этого же правила в исходном чате.",
    "Проверьте источник и получателя. Если это собственная публикация, действий не требуется.",
)
_SIZE = DeliveryError(
    "Файл слишком большой",
    "Размер файла превышает ограничение используемого способа отправки.",
    "Проверьте размер файла и доступный способ отправки; при необходимости уменьшите файл.",
)
_TIMEOUT = DeliveryError(
    "Превышено время ожидания",
    "Операция не завершилась вовремя; результат отправки может быть неизвестен.",
    "Проверьте получателя и состояние связи перед повторной отправкой.",
)
_ACCESS = DeliveryError(
    "Нет доступа к чату",
    "Telegram не разрешил операцию с каналом, группой или сообщением.",
    "Проверьте доступ бота и подключённого аккаунта, а также права на чтение или публикацию.",
)
_MISSING = DeliveryError(
    "Чат или сообщение не найдено",
    "Telegram не смог найти нужный чат или сообщение.",
    "Проверьте существование сообщения, подключение канала и права доступа.",
)
_MEDIA = DeliveryError(
    "Некорректный медиафайл",
    "Telegram отклонил медиафайл или не смог использовать его исходные данные.",
    "Проверьте исходный файл и возможность его воспроизведения; при необходимости замените его.",
)
_UNKNOWN = DeliveryError(
    "Не удалось определить причину",
    "Произошла ошибка, для которой пока нет точного описания.",
    "Передайте номер правила и сообщения администратору для проверки технического журнала.",
)
_NO_REASON = DeliveryError(
    "Причина не записана",
    "В истории доставки не сохранено описание ошибки.",
    "Проверьте технический журнал по номеру правила и сообщения.",
)

# Match stable codes before external exception patterns. A colon separates
# the code from arbitrary technical details, which are never rendered.
_CODES = {
    "album_member_set_changed_manual_review_no_resend": _RESCAN,
    "album_conflicting_member_vector_manual_review_no_resend": _RESCAN,
    "album_cache_member_provenance_unavailable_no_resend": _RESCAN,
    "album_membership_changed_pre_send_manual_review": _RESCAN,
    "copy_single_uncertain_no_fallback": _UNCERTAIN,
    "copy_album_uncertain_no_fallback": _UNCERTAIN,
    "telethon_send_attempted_result_unknown_non_retryable": _UNCERTAIN,
    "telethon_send_accepted_target_id_unresolved_non_retryable": _UNCERTAIN,
    "telethon_one_by_one_send_accepted_target_id_unresolved_non_retryable": _UNCERTAIN,
    "video_send_accepted_target_id_unresolved_non_retryable": _UNCERTAIN,
    "telethon_album_target_id_unresolved": _UNCERTAIN,
    "target_message_not_found_after_send": _UNCERTAIN,
    "self_loop_album_accepted_unverified_no_resend": _UNCERTAIN,
    "self_loop_single_accepted_unverified_no_resend": _UNCERTAIN,
    "self_loop_sent_id_collision": _UNCERTAIN,
    "self_loop_echo_delivery_already_quarantined": _ECHO,
    "self_loop_album_echo_fenced_before_send": _ECHO,
    "self_target_echo": _ECHO,
    "repost_original_document_invalid_non_retryable": _MEDIA,
    "rule_rescan_cancelled": DeliveryError(
        "Отменено пересканированием",
        "Старая задача остановлена при перестроении очереди.",
        "Проверьте завершение пересканирования и новую очередь правила.",
    ),
    "video_delivery_unsuccessful": DeliveryError(
        "Не удалось отправить видео",
        "Обработка или отправка видео завершилась без подтверждённого результата.",
        "Проверьте видео, канал-получатель и технический журнал.",
    ),
    "metrics_provider_not_configured": DeliveryError(
        "Диагностика недоступна", "Источник данных диагностики не настроен.",
        "Обратитесь к администратору.",
    ),
    "metrics_provider_failed": DeliveryError(
        "Диагностика недоступна", "Не удалось получить данные состояния доставки.",
        "Обновите диагностику позже; если ошибка повторяется, обратитесь к администратору.",
    ),
    "no_data": DeliveryError(
        "Нет данных", "Данные о доставках пока отсутствуют.",
        "Обновите диагностику позже.",
    ),
    "caption_or_edit": DeliveryError(
        "Пропуск по настройкам", "Сообщение исключено из отправки по настройкам подписи или редактирования.",
        "Проверьте настройки правила, если ожидали отправку этого сообщения.",
    ),
    "executor returned unsuccessful result": _UNKNOWN,
}

_PATTERNS = (
    (("accepted_target_id_unresolved", "target message id not confirmed", "result unknown", "failed_after_send"), _UNCERTAIN),
    (("request entity too large", "entity too large", "file is too big", "file too large", "payload too large", "file_part_too_big"), _SIZE),
    (("telegramretryafter", "retry_after", "retry after", "floodwait", "flood_wait", "flood wait", "too many requests", "rate limited", "rate_limit", "rate-limit", "slowmode"), _RATE_LIMIT),
    (("chat_write_forbidden", "chat_admin_required", "channel_private", "chat write forbidden", "chat admin required", "not enough rights", "bot is not a member", "bot was kicked", "forbidden", "protected content", "chat_forwards_restricted"), _ACCESS),
    (("chat not found", "message not found", "message_id_invalid", "message to copy not found", "peer_id_invalid", "channel_invalid"), _MISSING),
    (("document_invalid", "documentinvalid", "media_invalid", "media empty", "file_reference_expired", "file reference expired", "wrong file identifier", "invalid file", "moov atom not found", "invalid data found when processing input"), _MEDIA),
    (("timeout", "timed out", "timeouterror", "превышено время ожидания"), _TIMEOUT),
    (("connection reset", "connection refused", "networkerror", "network error", "server disconnected", "temporary failure", "bad gateway", "service unavailable"), DeliveryError(
        "Ошибка связи", "Не удалось завершить обмен данными с Telegram или сервером.",
        "Проверьте связь и канал-получатель перед повторной отправкой.",
    )),
    (("disk full", "no space left on device"), DeliveryError(
        "Недостаточно места на сервере", "Сервер не смог сохранить или обработать файл.",
        "Обратитесь к администратору для освобождения места.",
    )),
    (("ffmpeg", "ffprobe"), DeliveryError(
        "Ошибка обработки видео", "Не удалось подготовить видео к отправке.",
        "Проверьте исходный файл; при повторении обратитесь к администратору.",
    )),
    (("auth_key_unregistered", "authkeyunregistered", "session_revoked", "sessionrevoked", "session expired"), DeliveryError(
        "Сессия Telegram недействительна", "Подключённый аккаунт потерял авторизацию.",
        "Проверьте подключение аккаунта и восстановите авторизацию.",
    )),
    (("self-loop: confirmed publication echo",), _ECHO),
    (("не удалось получить весь альбом", "сообщение не получено"), DeliveryError(
        "Не удалось прочитать источник", "Бот не получил сообщение или полный альбом из Telegram.",
        "Проверьте доступ к источнику и наличие сообщений.",
    )),
    (("copy_message", "copy messages", "copymessage", "copy_", "copy "), DeliveryError(
        "Ошибка копирования", "Не удалось подтвердить копирование сообщения.",
        "Проверьте права и получателя перед повторной отправкой.",
    )),
)


def describe_delivery_error(value: object | None, error_type: object | None = None) -> DeliveryError:
    """Translate known reasons; never echo arbitrary exception text into Telegram."""
    parts = tuple(str(item).strip().lower() for item in (value, error_type) if item)
    if not any(parts):
        return _NO_REASON
    for part in parts:
        known = _CODES.get(part.split(":", 1)[0].strip())
        if known is not None:
            return known
    for markers, description in _PATTERNS:
        if any(marker in part for marker in markers for part in parts):
            return description
    return _UNKNOWN
