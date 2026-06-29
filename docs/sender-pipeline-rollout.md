# Sender Pipeline Rollout

## 1. Цель

Stage 24 вводит безопасный foundation для будущего production rollout новых sender pipeline.
Модуль описывает режимы включения, allowlist/denylist по `rule_id`, безопасное решение для логов и конвертацию rollout-конфига в `SenderPipelineFeatureFlags`.

Важно: этот этап не подключает новые pipeline к реальной отправке и не меняет runtime-поведение.

## 2. Почему все выключено по умолчанию

Production bot уже отправляет сообщения через существующий legacy path.
Любое изменение delivery path может создать риск дублей, пропусков, неверного rollback или ошибок в очереди.
Поэтому default-конфиг — fail-closed:

- режим `DISABLED`;
- `rollout_percent = 0`;
- allowlist правил включён;
- active без `rule_id` запрещён;
- feature flags выключены.

## 3. Режимы DISABLED / SHADOW / DRY_RUN / ACTIVE

`DISABLED` означает, что новый pipeline не должен вызываться.

`SHADOW` означает, что в будущем pipeline можно будет вызвать для сравнения, но legacy path остаётся источником истины.

`DRY_RUN` означает, что можно строить input/decision/logs без Telegram side effects.

`ACTIVE` означает, что в будущем pipeline может стать реальным путем отправки, но только через guards и allowlist.

## 4. Что означает legacy path

Legacy path — текущая production-логика отправки, очереди, audit, delivery attempts, rollback и retry.
Stage 24 не меняет этот путь и не добавляет runtime wiring.

## 5. Что означает shadow

Shadow — безопасное сравнение нового pipeline с legacy path.
В shadow-режиме результат нового pipeline не используется для production delivery.
Legacy path продолжает быть источником истины.

## 6. Что означает dry-run

Dry-run — режим планирования и проверки решения без вызова pipeline с Telegram side effects.
Он предназначен для подготовки input, логов и проверки guards.

## 7. Что означает active

Active — режим, при котором новый pipeline может использоваться как реальный путь.
Для Stage 24 это только decision model: реального подключения к отправке нет.

## 8. Почему ACTIVE только через allowlist rule_id

`ACTIVE` должен включаться только на ограниченных правилах.
Это снижает blast radius и позволяет быстро проверить конкретный `rule_id` без глобального влияния на production.
По умолчанию `require_rule_allowlist=True`, поэтому active требует явного `enabled_rule_ids`.

## 9. Как откатываться

Быстрый rollback должен быть простым:

1. перевести mode в `DISABLED`;
2. убрать pipeline из enabled/shadow/dry-run списков;
3. добавить проблемный `rule_id` в `blocked_rule_ids`;
4. проверить очередь, delivery attempts и audit logs;
5. продолжить legacy path.

`blocked_rule_ids` должен иметь приоритет над enabled/shadow/dry-run списками.

## 10. Безопасный порядок будущего включения

1. Выбрать один тестовый `rule_id`.
2. Включить `SHADOW` для выбранного pipeline и rule_id.
3. Проверить shadow-результаты без использования их для доставки.
4. Включить `DRY_RUN`, чтобы проверить input/decision/logs без side effects.
5. Включить `ACTIVE` только для тестового `rule_id`.
6. Наблюдать метрики, audit и delivery attempts.
7. Расширять allowlist постепенно, небольшими шагами.

## 11. Запрещено

- включать globally без allowlist;
- менять SQL/runtime в rollout stage;
- подключать rollout без regression tests;
- подключать rollout напрямую к Telegram handlers/admin UI без отдельного этапа;
- менять очередь, scheduler, audit или delivery attempts в rollout foundation.

## 12. Какие метрики смотреть перед включением

Перед расширением rollout нужно смотреть:

- `pending` / `processing` / `faulty`;
- rate-limited / deferred состояния;
- duplicate risk;
- `delivery_attempts`;
- audit logs;
- частоту retry и rollback;
- расхождения между legacy и shadow результатами.

## 13. Pipeline names

Rollout использует явные имена pipeline:

- `repost_single`;
- `repost_album`;
- `video_send`;
- `legacy_video_delivery`;
- `repost_campaign`;
- `reactions`.

## 14. Safe logs

Decision log context содержит только технические поля: pipeline, mode, action, ids, flags и reason.
В логи нельзя добавлять raw payload, caption, content_json, Telegram objects, DB objects, tokens или secrets.


## Stage 26 — Delivery diagnostics admin integration v1

Stage 26 adds a read-only admin UI integration for delivery diagnostics. The admin diagnostics menu can show a safe Russian DeliveryDiagnosticsSnapshot report built from repository delivery metrics. This stage does not change sender runtime, worker runtime, Telegram send gateway, rollout strategy activation, delivery attempts behavior, scheduler behavior, database schema, or migrations.


## Stage 27 — RepostSingle rollout probe / shadow-safe runtime integration v1

Stage 27 подключает runtime decision для одиночного repost через `SenderPipelineRolloutStrategy`, но не переводит отправку на новый pipeline. Env builder поддерживает `SENDER_PIPELINE_ROLLOUT_MODE`, `SENDER_PIPELINE_ROLLOUT_REPOST_SINGLE_RULE_IDS` и `SENDER_PIPELINE_ROLLOUT_BLOCKED_RULE_IDS`; default остаётся `disabled`, `rollout_percent=0`, `require_rule_allowlist=True`, `fail_closed=True`. В `dry_run`/`shadow` probe только проверяет наличие безопасных полей для будущего `RepostSingleInput` и пишет safe log. В `active` Stage 27 возвращает `active_not_enabled_in_stage_27` и продолжает legacy path. `RepostSinglePipeline.run()` не вызывается, Telegram copy/send через новый pipeline не выполняется.

## Stage 28.2 — RepostSingle active canary reactions post-send

Stage 28.2 allows the `RepostSingle` active canary to use the existing SenderService reaction post-send flow after a successful active pipeline send. Reactions are no longer treated as `unsupported_feature` for the `RepostSingle` active canary: a rule that uses the standard reaction runtime can pass the active canary guards when the rest of the single repost requirements are safe.

When `RepostSingleActiveCanaryRunner` attempts `RepostSinglePipeline.run()`, legacy copy/send remains stopped for that delivery. If the active pipeline returns usable `sent_message_ids`, SenderService uses the first id as authoritative, runs the existing reaction post-send step, marks the delivery sent, and touches the rule. Reaction post-send warnings are non-fatal after the active send; they never trigger a legacy copy fallback or a second `copy_message`.

Active failure, exception, or handled-without-sent-ids still fails closed with manual review / faulty delivery behavior and no legacy fallback. Dry-run and shadow rollout remain read-only for Telegram writes, active still requires `SENDER_PIPELINE_REPOST_SINGLE_ACTIVE_CANARY_ENABLED=1`, and active canary still requires exactly one allowlisted rule id.
