# ViMi Autoposting: аудит надёжности доставок (23.11.2026)

Основа: read-only отчёт production PostgreSQL за 24.10–23.11.2026 и `main` @ `0fccd386`.
Задача: устранить случайные дубли и отдельные подписи, **не нарушить** запланированные повторы в `fixed` и работу self-loop.

## 1. Проверенные звенья
`scheduler_runtime.scheduler_tick` → `PostgresRepository.take_due_delivery_and_create_job`
→ `jobs` / `worker_runtime._process_job` → `SenderService.execute_repost_single_from_job / execute_repost_album_from_job / video`
→ `RepostSingleDelivery / RepostAlbumDelivery / VideoSendDelivery`
→ `SenderBotApiCopyHelpers / SenderReuploadHelpers / SenderTelethonHelpers`
→ `TelethonAuthoritativeMessageResolver` → `delivery_attempts`, `deliveries`, `audit_log`.

Также: `job_watchdog`, `parser`, `delivery_diagnostics_admin`, `admin_handlers/diagnostics`, `sender_state_sync_helpers`, `delivery_idempotency`, `telegram_send_result`, `video_pipeline_stages`.

## 2. Факты production
- 95 правил, среди них 3 точных self-loop (#14, #149, #259); 3 `fixed` (#14, #19, #149). Совпадение чата с разными темами не считается точным self-loop.
- 9 `faulty` доставок, созданных в пределах 30 дней; 135 неуспешных jobs (102 с `telethon_send_accepted_target_id_unresolved_non_retryable`), 113 попыток `failed_before_send / failed_after_send`.
- 9 485 «uncertain» записей — **не 9 485 ошибок**: 9 375 имеют `accepted`, 6 589 привязаны к `sent`. 2 887 без связанной доставки требуют отдельного расследования/сверки архивирования, но не массового replay.
- 72 события `text_fallback`: 36 `started` и 36 `ok`; 33 уникальных `delivery_id` среди успехов. У трёх доставок по **два успеха с разными Telegram ID**:
  - #31: delivery 35317, target ID 1764/1766;
  - #183: delivery 1138667, target ID 1063/1064;
  - #267: delivery 1416754, target ID 25/27.
  Факт двойного вызова успешной отправки подтверждается журналом; содержимое в Telegram не сверено.
- #14 (`fixed+self-loop`): 305 `sent` за период; у 304 из них нет persisted `sent_message_id`. #149 (`fixed+self-loop`): за период 0 `sent`, 32 failed jobs. Это не доказывает отсутствие реальных сообщений в Telegram.
- 374 `sent` записей фиксированных правил за период; `fixed_repeated_source_candidates=0` означает только отсутствие повторов одного исходного ID в сохранённых deliveries.
- Битые исходные MP4 и ограничения `FloodWait` есть, но требуют **других** веток восстановления.

## 3. Дефекты, присутствующие в текущем коде
1. `RepostSingleDelivery.deliver`: `text_fallback` успешен, но `_mark_delivery_sent_sync(delivery_id)` не сохраняет уже известный `sent.message_id` / метод отправки. Результат расходится с delivery ledger.
2. `RepostSingleDelivery.deliver`: при неудаче отправки исходного *медиа* допускается отправка его исходного текста отдельно; `media_downgrade_blocked` до исправления применялся только к активной пользовательской подписи. Это потенциально порождает «отдельную подпись», когда медиа фактически было принято, но проверка не прошла.
3. `PostgresRepository.clear_faulty_delivery_log`: кнопка «Очистить лог» действительно удаляет строку `deliveries`. Это не просто очистка журнала; теряются привязки и первичные данные для reconciliation. Исправление: мягкое скрытие через существующий `audit_log`, без UPDATE/DELETE deliveries и миграций.
4. `get_faulty_deliveries` выводит только текущие `faulty`, не охватывает failures из jobs/attempts и предупреждения после успешной отправки. Не считать эту кнопку полным обзором системы.

## 4. Уже имеющиеся механизмы, сохраняемые без изменений
- Планировщик атомарно резервирует доставку и работу, использует `FOR UPDATE ... SKIP LOCKED`.
- `delivery_attempts.idempotency_key` уникален, `mark_delivery_attempt_sending` является CAS переходом.
- Worker имеет heartbeat и owner-fenced `finalize_owned_job`.
- `accepted` и `failed_after_send` защищены от слепого повторного Telegram send в executor; попытки расхождения передаются в manual review.
- Проверка target message ID и проверка source=target учитывают ID и темы. Реальные повторы по плану `fixed` не должны быть глобально запрещены.

## 5. Следующий архитектурный рубеж (отдельно от точечных фиксов этого PR)
1. Единый state machine `planned → claimed → sending → accepted → verified/sent` и `uncertain_manual_review`. CAS **до** каждого внешнего send везде, в том числе в прямых/резервных путях, не только в job executor.
2. Стабильный логический ключ доставки, а для фиксированных повторов уникальный ключ **на каждый плановый запуск**. Не объединять намеренные новые публикации.
3. Запись неизменяемого send receipt (target chat/thread, source ids, returned candidate, method, timestamps, API request context) **до** post-send реакций, редактур и подтверждений. После успешной отправки реакция/редактирование не должны инициировать повторный send.
4. При таймауте и неопределённом результате Bot API — reconciliation или ручная проверка, **без автоматической отправки вторым методом**. У Bot API нет общего server-side idempotency token.
5. Для MTProto изучить применение низкоуровневого неизменного `random_id` одного logical send (проверить совместимость Telethon и медиагрупп); произвольно повторять высокоуровневый send опасно.
6. Self-loop: исключать из повторного захвата сообщения, опубликованные самим сервисом, используя ledger target IDs и источников, но не запрещать самостоятельную публикацию пользователя и фиксированные повторные запуски. Нужна проверка обновлений Telegram для источника.
7. Recovery: автоматические retries только для ошибок **достоверно до отправки**; возможное принятие → manual reconciliation. Не requeue подтверждённое/неопределённое без подтверждения target.
8. Наблюдаемость: раздельно учитывать `final_failure`, `transient_retry`, `skipped_by_policy`, `accepted_unverified`, `post_send_warning`; по одной проблеме на delivery; никакого удаления доказательств.

## 6. Матрица тестирования перед следующим расширением
- Одновременно 2–4 worker на одной доставке; контекстные switches между Telegram response, сохранением receipt, verify, marking sent.
- Сообщение принято, но HTTP timeout; MTProto timeout; Telegram возвращает неверный или отсутствующий ID; worker crash после принятия.
- Альбом с частичным принятием (не превращать в «доставлено всё»); видео с битым moov atom; FloodWait/backoff с jitter.
- Text-only, real media, link-preview, custom caption on/off, форматирование entities; после post-send ошибки реакции.
- Self-loop тот же канал и та же тема; один чат разные темы; обычная пересылка; fixed 1x/2x в сутки без запрета намеренных повторов.
- Отчёт об ошибке нельзя «очистить» удалением `deliveries`; журнал и receipts сохраняются.

## 7. Практика и первоисточники
- Telegram Bot API: `copyMessage` возвращает `MessageId` и поддерживает caption; `copyMessages` сохраняет альбом, но не имеет override подписи: https://core.telegram.org/bots/api
- MTProto random_id и дедупликация запросов: https://core.telegram.org/api/updates
- PostgreSQL `SKIP LOCKED` для очередей: https://www.postgresql.org/docs/current/sql-select.html
- AWS Builders' Library, retries safe with idempotent APIs: https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/

## 8. Границы исправляющего PR
Эта версия фиксирует утрату receipt при `text_fallback`, запрещает media→text downgrade с «оторванной подписью» и делает очистку ошибок **недеструктивной**. Не изменяет очереди, расписания, self-loop или схему БД; **не запускает автоматические повторы** старых доставок. Более глубокая унификация `sending/accepted/verified` требует отдельной реализации и тестов с имитируемой неопределённостью Telegram.
