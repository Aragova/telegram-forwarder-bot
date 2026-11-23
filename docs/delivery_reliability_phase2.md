# Повторные отправки и безопасность доставки: этап 2 (23.11.2026)

Эта правка продолжает PR #522. Основание: read-only выгрузка production и аудит цепочки Python→PostgreSQL→Telethon/Bot API.

## 1. Почему возникали дубли
В `SenderTelethonHelpers.send_file_via_telethon` и `send_album_via_telethon` исключение во время первого вызова `telethon.send_file` ранее приводило к повторной отправке файла по другому методу. Сетевой timeout/ConnectionReset после вызова **не доказывает**, что Telegram не принял запрос. Аналогично `SenderReuploadHelpers` допускал Bot API fallback после MTProto `transport_attempted=True, transport_accepted=False`, а отправка альбома по одному элементу могла продолжить/повторить частичную публикацию.

Исправление: после начала внешнего `send_file` исключение означает `transport_attempted=True` и **блокирует все последующие transport send** для того же delivery; запись `faulty` и manual reconciliation вместо replay. При достоверном отсутствии попытки внешнего send оставляем резервный путь. Наличие записи `faulty` НЕ означает, что сообщение отсутствует в Telegram.

## 2. Self-loop
`_is_self_target_echo_post_for_rule_conn` уже защищал от части эха, но проверял только `deliveries.sent_message_id`. Теперь он также учитывает сохранённый массив ID альбома и `verified` ID в `delivery_attempts`, при точном совпадении правила и получателя. **Новые исходные посты от пользователя и фиксированные запланированные повторы не запрещаются.** Исторические посты без receipts нуждаются в ручной сверке, а не массовом пропуске.

## 3. Подписи
PR #522 уже запретил отправлять отдельный текст при неудачной доставке реального медиа и сохраняет ID текстового fallback. Здесь добавлена защита от повторного пути после неоднозначной MTProto-отправки текста/медиа/альбома.

## 4. Диагностика
У события `copy_single`/ `copy_album` с `pipeline_result=skipped` выбранная политика подписи ранее записывалась как `error_text`, увеличивая число ложных ошибок в аудитах. Теперь у таких событий `error_text=NULL`, а `skip_reason` остаётся в `extra_json`. Старые записи остаются неизменными. Карточки `faulty` лучше помечают неподтверждённый результат Telegram и надёжнее отображают дату.

## 5. Видео
`ffprobe` проверяет теперь наличие **видеопотока**, а не только любых streams/format. Отсутствие бинарника / сбой подпроцесса ffprobe классифицируется как **временная ошибка валидации**, исходный файл не удаляется, ошибка возвращается как retryable. Достоверно повреждённые MP4 по-прежнему удаляются из временного кэша и проходят ограниченные попытки повторного скачивания.

## 6. Что НЕ делает этот PR
- Не повторяет автоматически старые `faulty`, не меняет расписания, число правил, цены, БД или схему.
- Не гарантирует «exactly once»: у Telegram Bot API нет универсального idempotency key. Безопасное поведение при неизвестном результате: **manual reconciliation / no second send**.
- Не скроет старые ошибки без решения оператора. Для #14/#149/#94/#253/#254/#267 требуется отдельная проверка принятых сообщений и ledger.

## 7. Проверки
Регрессионные тесты проверяют: `ConnectionReset` после начала RPC, отсутствие второго вызова Telethon и Bot API, сохранение self-loop receipts для альбомов, корректную классификацию skip, неисправный `ffprobe`, отсутствие видеопотока и сохранность исходного файла при временном сбое.

## Практика
- AWS Builders' Library: retries must be safe with idempotent APIs: https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/
- PostgreSQL `FOR UPDATE SKIP LOCKED`: https://www.postgresql.org/docs/current/sql-select.html
- Telegram MTProto `random_id`: https://core.telegram.org/method/messages.sendMessage
- Telegram Bot API `sendMessage`/`copyMessage`: https://core.telegram.org/bots/api
