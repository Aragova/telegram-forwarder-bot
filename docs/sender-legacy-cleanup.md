# Sender Legacy Cleanup Plan

## 1. Цель

Stage 25 фиксирует карту legacy cleanup перед реальным подключением Sender Architecture v2.
Документ и `app/sender_legacy_inventory.py` отвечают на практические вопросы:

- что в `SenderService` считается legacy;
- что можно переводить первым;
- что нельзя трогать без отдельного этапа;
- какие fallback и rollback гарантии должны сохраниться;
- где есть риск дублей, потерь и Telegram side effects.

Stage 25 не подключает новые pipelines к runtime и не меняет отправку.

## 2. Почему legacy нельзя удалять сразу

Legacy path сейчас является рабочим production path для repost, album, video, campaign и reactions.
Удаление без phased rollout может привести к:

- повторной отправке сообщений;
- потере статусов доставки;
- рассинхронизации `delivery_attempts`;
- неверному `pending/processing/faulty` состоянию очереди;
- невозможности отката после Telegram write side effects.

## 3. Что считается legacy в SenderService

Legacy-зоны в `SenderService`:

- single repost delivery: `_deliver_single`;
- album repost delivery: `_deliver_album`;
- video delivery: `_deliver_single_video`;
- legacy video bridge/download/process/send path: `_download_video_source` и связанные этапы;
- repost campaign copy/delete jobs;
- reactions после отправки;
- attempt ledger и `delivery_attempts`;
- target verification после Telegram write;
- finalization, post-send steps и audit events;
- scheduler touch и queue/status updates;
- caption/entities/content handling;
- transport boundary: copy/send/reupload/media group Telegram writes.

## 4. Почему fallback должен остаться до полного rollout

Fallback нужен до тех пор, пока новый path не докажет parity на shadow и active allowlist.
Он должен оставаться fail-closed: если rollout config отсутствует, ошибочен или не разрешает правило, используется legacy path.
Fallback также нужен для быстрого rollback без миграций и без изменения очереди.

## 5. Порядок будущего перевода

1. single repost shadow: сравнивать решения без Telegram writes;
2. single repost active for test rule: включить только для тестового rule allowlist;
3. single repost allowlist expansion: расширять постепенно по правилам;
4. album repost: переносить после single parity и проверки album ordering;
5. video delivery: переносить отдельно из-за ffmpeg/download/send side effects;
6. reactions: переносить отдельно, reaction logic не смешивать с delivery cleanup;
7. campaign cleanup: copy/delete переносить только после проверки delete side effects;
8. final sender.py cleanup: удалять legacy methods только после полного active rollout.

## 6. Что можно трогать первым

Первым кандидатом является single repost path вокруг `_deliver_single`.
Допустимый первый шаг Stage 26+: shadow/diagnostics через feature flags и rule allowlist.
Caption/content helpers можно проверять в shadow, если они не выполняют Telegram writes.

## 7. Что нельзя трогать пока

Пока нельзя менять:

- video delivery и legacy video processing bridge;
- queue/status source of truth;
- scheduler touch;
- reactions;
- transport policy;
- repository/Postgres schema, SQL и migrations;
- audit log и problem_state.

## 8. Rollback plan

Rollback должен быть быстрым и безопасным:

- выключить feature flag или убрать rule из allowlist;
- убедиться, что config остаётся fail-closed;
- оставить legacy path доступным для всех delivery methods;
- не удалять old delivery methods до подтверждённой parity;
- проверять отсутствие дублей после отката.

## 9. Safety gates перед удалением legacy path

Перед удалением legacy path нужны gates:

- shadow parity без Telegram write side effects;
- controlled active rollout по одному rule;
- отсутствие duplicate delivery;
- корректная finalization mapping;
- target verification защищает от false success;
- `delivery_attempts` остаётся консистентным;
- queue diagnostics не показывают stuck `pending/processing/faulty`;
- rollback проверен на реальном allowlist сценарии.

## 10. Обязательные проверки

Перед каждым практическим этапом запускать:

- full pytest;
- selected sender/pipeline tests;
- no duplicate delivery check;
- delivery_attempts consistency;
- queue pending/processing/faulty diagnostics;
- rollout config fail-closed.

## 11. Как использовать SenderPipelineRolloutStrategy

`SenderPipelineRolloutStrategy` должен оставаться gatekeeper для rollout decisions.
Новые подключения должны идти через feature flags, rule allowlist и fail-closed поведение.
Нельзя обходить strategy ручными проверками внутри delivery methods.

## 12. Как использовать DeliveryObservabilityService

`DeliveryObservabilityService` использовать для диагностики readiness, stuck states и rollback signals.
На Stage 25 он не подключается к admin UI, live dashboard или runtime handlers.
В будущих этапах diagnostics должны помогать принять решение, но не менять отправку сами по себе.

## 13. Definition of done перед реальным удалением legacy methods

Legacy methods можно удалять только когда выполнено всё:

- новый path прошёл shadow и active rollout;
- все затронутые tests зелёные;
- нет расхождений по sent message ids;
- нет duplicate sends/copies/reuploads;
- delivery_attempts и audit events совпадают с ожиданиями;
- rollback проверен и документирован;
- video/reactions/campaign перенесены отдельными этапами;
- owner подтвердил, что legacy fallback больше не нужен.


## Stage 26 — Delivery diagnostics admin integration v1

Stage 26 adds a read-only admin UI integration for delivery diagnostics. The admin diagnostics menu can show a safe Russian DeliveryDiagnosticsSnapshot report built from repository delivery metrics. This stage does not change sender runtime, worker runtime, Telegram send gateway, rollout strategy activation, delivery attempts behavior, scheduler behavior, database schema, or migrations.
