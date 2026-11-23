# Повторные отправки и безопасность доставки: этап 2 (23.11.2026)

Эта правка продолжает PR #522. Основание: read-only выгрузка production и аудит цепочки Python→PostgreSQL→Telethon/Bot API.

## 1. Почему возникали дубли
В `SenderTelethonHelpers.send_file_via_telethon` и `send_album_via_telethon` исключение во время первого вызова `telethon.send_file` ранее приводило к повторной отправке файла по другому методу. Сетевой timeout/ConnectionReset после вызова **не доказывает**, что Telegram не принял запрос. Аналогично `SenderReuploadHelpers` допускал Bot API fallback после MTProto `transport_attempted=True, transport_accepted=False`, а отправка альбома по одному элементу могла продолжить/повторить частичную публикацию.

Исправление: после начала внешнего `send_file` **сетевая ошибка или неизвестный результат** означает `transport_attempted=True` и блокирует все последующие transport send для того же delivery; запись `faulty` и manual reconciliation вместо replay. Только явно отвергнутые Telegram запросы (`BadRequest`, HTTP 400) могут переключаться на резервный путь без риска дубля. Наличие записи `faulty` НЕ означает, что сообщение отсутствует в Telegram.

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

### Сохранение Telegram ID (дополнение)
Подтверждённые отправки альбомов через Copy, повторную попытку Copy, Reupload и one-by-one сохраняют `sent_message_ids_json` и ID каждого элемента через `_mark_album_deliveries_sent_sync`. Это помогает отделить эхосообщения self-loop от новых постов пользователя и повышает качество последующей сверки.

## 8. Подтверждённый инцидент: worker прервал длительную загрузку #267

Для `delivery_id=1416848` журнал за 23.11.2026 показывает, что обработчик находился в `await telethon.send_file` свыше пяти минут. Во время завершения процесса прежний 20-секундный graceful shutdown отменил незавершённую операцию. Результат отправки остался неизвестным, а job watchdog восстановил осиротевшее задание. Автоматический повтор запрещён из-за риска дубликата.

### Изменение механизма остановки

1. `stop_job_workers_runtime` больше не вызывает `task.cancel()` сразу. Сначала выставляет `stop_event` и ждёт worker-drain.
2. Worker не арендует новые задания после сигнала stop. Если аренда уже завершалась при shutdown, он возвращает ещё **не запущенное** задание в очередь без новой Telegram-отправки и без увеличения счётчика attempts (через `defer_job`).
3. Активным задачам отведено до **600 секунд** для записи результата и корректной остановки. Ожидание верхнего слоя составляет до 620 секунд; после дедлайна задание отменяется, а результат остаётся неопределённым. Это **не разрешение на replay**.
4. Для systemd нужен запас больше предельного ожидания Python: `TimeoutStopSec=720`, `KillSignal=SIGINT`. Split worker-service содержит настройки; для работающего legacy `forwarder_bot.service` добавлен drop-in `deploy/systemd/forwarder_bot.service.d/20-graceful-drain.conf`.

### Критически важно при деплое

Кодовый `git pull` **не устанавливает автоматически** systemd drop-in. Перед рестартом новой версии установить override для **реально активного** сервиса и выполнить `systemctl daemon-reload`. Без этого systemd может прервать даже исправленный Python-drain по собственному короткому таймауту. После обновления проверить `systemctl show forwarder_bot.service -p TimeoutStopUSec -p KillSignal`; в журнале должны быть `WORKER_DRAIN_START`, `WORKER_DRAIN_COMPLETE` (или `WORKER_DRAIN_TIMEOUT` при исключительной ситуации).

Проверка не изменяет посты или историю. Старую `faulty` запись #267 следует сверить с получателем вручную, не отправлять автоматически.
