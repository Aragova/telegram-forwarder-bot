# Sender Architecture v2 — Architecture Baseline

## 1. Purpose

This document records the target architecture for gradually reducing the responsibilities of `app/sender.py` and for defining the future Telegram transport boundary around the existing transport layer.

This is a baseline document for future implementation stages. It does not change runtime code, production behavior, database schema, job payloads, or public worker-facing sender contracts.

The main goal of Sender Architecture v2 is to make delivery behavior safer and easier to reason about before connecting and evolving the already existing transport components in `app/transport.py` and `app/transport_policy.py`.

## 2. Current state

The current delivery flow is effectively:

```text
worker_runtime.py
→ SenderService
→ sender.py
→ raw Bot / raw Telethon
→ Telegram API / Telethon API
→ verify / reaction / DB finalization / audit / schedule
```

`app/sender.py` is currently an overloaded delivery-layer file. It knows too much and does too much in one place, including:

```text
job entrypoints
Telegram Bot API calls
Telethon calls
copy/reupload logic
caption strategy
entity/custom emoji processing
target verification
delivery_attempts ledger coordination
delivery finalization
audit logging
schedule touch
reaction handling
video send/fallback handling
campaign copy/delete handling
```

This makes `sender.py` the main risk concentration point for delivery bugs. Future work must reduce this risk through small, safe, staged extraction and must not rewrite behavior opportunistically.

## 3. Existing building blocks

The repository already contains important delivery and transport building blocks. Future stages should reuse and evolve them instead of creating parallel abstractions.

### `app/delivery_idempotency.py`

Role: builds delivery idempotency keys and normalizes sent message ids into a safe, valid list form.

Current responsibilities include:

- `build_delivery_idempotency_key()` for stable operation keys.
- `normalize_valid_sent_message_ids()` for filtering sent ids.
- `extract_sent_message_ids_from_attempt()` for reading ids from existing attempts.

### `app/telegram_send_result.py`

Role: normalizes raw Telegram send/copy results into a safe sender-side contract with valid sent message ids.

Current responsibilities include:

- `TelegramSendResult` as the canonical Telegram-send boundary result.
- `telegram_send_result_from_raw()` for converting raw Bot/Telethon results.
- `telegram_send_success()` and `telegram_send_failure()` helpers.

### `app/delivery_result.py`

Role: sender → worker contract.

Current responsibilities include:

- `DeliveryResult` as the canonical public result returned to worker runtime.
- Dict normalization through `delivery_result_from_dict()` and `normalize_delivery_result()`.
- Boolean compatibility through `delivery_result_to_bool()`.

### `app/transport.py`

Role: proxy wrappers for Bot and Telethon clients.

Current responsibilities include:

- `BotTransportProxy`.
- `TelethonTransportProxy`.
- `wrap_bot()`.
- `wrap_telethon_client()`.

These wrappers are the existing place for routing client method calls through a transport policy.

### `app/transport_policy.py`

Role: retry, rate-limit, backoff, and concurrency policy.

Current responsibilities include:

- `TransportPolicy`.
- `TransportRateLimited`.
- retry/backoff/error classification.
- sender-specific builders: `build_sender_bot_policy()` and `build_sender_telethon_policy()`.
- separate policy builders for reaction and video clients.

### `app/worker_runtime.py`

Role: job execution, `DeliveryResult` normalization, idempotency guard, retry/fail/defer decisions, worker metrics, and cleanup for some video artifacts.

The worker must keep using public sender methods that return compatible `DeliveryResult` data.

### `app/video_processor.py`

Role: video processing and video send helper layer.

It owns the ffmpeg-oriented processing pipeline and video-specific send helpers. Sender Architecture v2 must not casually rewrite or duplicate this pipeline.

### Other relevant files reviewed for this baseline

- `app/scheduler_runtime.py`: scheduler tick and scheduler loop for enqueueing due work.
- `app/job_service.py`: job payload construction, dedup keys, and enqueue helpers.
- `app/postgres_repository.py`: delivery state, delivery attempts, job leasing, logical queue, rollback, start-from-position, audit, and persistence operations.

## 4. Transport layer status

Transport layer already exists, but must not be duplicated.

Future work must develop and connect the existing transport layer instead of introducing another transport abstraction with different naming or duplicated behavior. The existing components to preserve and evolve are:

```text
BotTransportProxy
TelethonTransportProxy
wrap_bot
wrap_telethon_client
TransportPolicy
TransportRateLimited
build_sender_bot_policy
build_sender_telethon_policy
```

A critical risk is that Telegram send/copy operations are non-idempotent external side effects. Blind automatic retry of send/copy operations may create duplicate posts.

Examples of non-idempotent Telegram write operations include:

```text
copy_message
send_message
send_photo
send_video
send_document
send_media_group
send_file
```

Transport policy must therefore become operation-aware before it is broadly applied to sender write calls.

## 5. Target architecture

The target Sender Architecture v2 flow is:

```text
worker_runtime.py
→ SenderService facade
→ DeliveryContext
→ DeliveryPipeline
→ AttemptLedgerService
→ TelegramSendGateway
→ TelegramTransportBoundary
→ TelegramSendResult
→ TargetVerifier
→ PostSendSteps
→ DeliveryFinalizer
→ DeliveryResult
→ worker_runtime.py
```

This is a target model, not the current runtime shape. It should be introduced stage by stage while preserving production behavior.

## 6. Target responsibilities

### SenderService facade

`SenderService` should become a thin facade.

The public worker-facing methods must remain compatible:

```text
execute_repost_single_from_job
execute_repost_album_from_job
execute_video_download_from_job
execute_video_process_from_job
execute_video_send_from_job
execute_video_delivery_from_job
execute_repost_campaign_send_copy_from_job
execute_repost_campaign_delete_copy_from_job
```

In the target design these methods should only create a delivery context, call the appropriate pipeline, and return a compatible result.

### DeliveryContext

`DeliveryContext` is the future single context object for one logical delivery operation.

Expected fields:

```text
delivery_id
rule_id
tenant_id
job_id
source_channel
source_thread_id
target_id
target_thread_id
message_id
media_group_id
mode
schedule_mode
interval
operation_kind
idempotency_key
```

### DeliveryPipelineResult

`DeliveryPipelineResult` is the future internal pipeline result.

Expected fields:

```text
ok
stage
method
sent_message_ids
accepted
verified
retryable
manual_review_required
error_text
warnings
delivery_method
finalize_mode
```

It must remain internal. `DeliveryResult` remains the canonical sender → worker boundary contract.

### TelegramSendGateway

`TelegramSendGateway` is the future layer for Telegram API calls.

It should:

```text
использовать sender_bot / sender_telethon
прогонять вызовы через transport boundary
возвращать TelegramSendResult
не писать delivery sent
не делать touch_rule_after_send
не ставить reactions
не решать финальное состояние доставки
```

### AttemptLedgerService

`AttemptLedgerService` is the future layer above `delivery_attempts`.

It should centralize:

```text
cache hit
create attempt
mark sending
mark accepted
mark failed_before_send
mark failed_after_send
```

Main invariant:

```text
accepted / verified attempt must not be overwritten by failed status.
```

### TargetVerifier

`TargetVerifier` is the future target-message verification layer.

It should:

```text
validate sent message ids
confirm target delivery
verify album delivery
validate reaction target
```

It must not send messages.

### PostSendSteps

`PostSendSteps` is the future post-send operations layer:

```text
target verify warnings
reaction enqueue/apply
post-send audit warnings
```

Main invariant:

```text
After Telegram send is accepted, post-send errors must not trigger resend.
```

### DeliveryFinalizer

`DeliveryFinalizer` is the future final delivery state layer.

Only this layer should do:

```text
mark_delivery_sent
mark_many_deliveries_sent
mark_delivery_faulty
touch_rule_after_send
final success audit
final failure audit
```

Main invariant:

```text
Schedule touch must happen once per logical delivery finalization.
```

## 7. Transport target model

Transport should become operation-aware before broad sender integration.

Operations should be separated conceptually:

```text
safe_read
download
verify
non_idempotent_write
reaction
unknown
```

Examples:

```text
get_messages → safe_read
download_media → download
copy_message → non_idempotent_write
send_file → non_idempotent_write
send_media_group → non_idempotent_write
reaction operations → reaction
```

Main principle:

```text
non_idempotent_write operations must not be blindly auto-retried.
```

## 8. Critical invariants

1. Do not duplicate existing transport.py / transport_policy.py.
2. Do not wrap the global UI bot with sender transport policy.
3. Sender transport must use sender-specific wrapped clients.
4. Telegram write side effects must not be blindly retried.
5. delivery_attempts accepted must be written immediately after valid sent ids.
6. Post-send verify/reaction errors must not cause resend after accepted.
7. Delivery finalization must be centralized.
8. touch_rule_after_send must happen once per logical delivery.
9. Worker public sender methods must stay compatible.
10. Job payload schema must stay compatible unless explicitly migrated.
11. Album logical item must be finalized as one logical delivery.
12. Reaction clients must not be mixed with sender transport without a separate step.
13. DeliveryResult and TelegramSendResult must remain canonical boundary contracts.

## 9. Planned stages

0. Architecture baseline / no runtime changes
1. Transport inventory and tests
2. Operation-aware transport policy
3. Safe sender transport policies
4. Connect transport only to SenderService
5. Transport observability
6. DeliveryContext v1
7. DeliveryPipelineResult v1
8. Pure helpers extraction: caption/entities/content
9. TelegramSendGateway v1
10. TargetVerifier v1
11. AttemptLedgerService v1
12. PostSendSteps v1
13. DeliveryFinalizer v1
14. RepostSinglePipeline
15. RepostAlbumPipeline
16. VideoSendPipeline
17. LegacyVideoDeliveryPipeline
18. RepostCampaignPipeline
19. ReactionPostSendService
20. SenderService as thin facade
21. Repository responsibility cleanup
22. Delivery observability dashboard/diagnostics
23. Regression test matrix
24. Production rollout strategy / feature flags
25. Legacy cleanup

## 10. What this stage must NOT do

This stage must not:

```text
изменять app/sender.py runtime logic
изменять app/transport.py behavior
изменять app/transport_policy.py behavior
подключать transport к SenderService
создавать новые production modules
переписывать worker_runtime.py
переписывать video_processor.py
менять delivery_attempts
менять DeliveryResult
менять TelegramSendResult
менять job payload schema
менять Postgres schema
удалять legacy code
делать feature flags
```

Runtime code was not changed.
Transport layer was not duplicated.
This stage only documents the agreed Sender Architecture v2 baseline.


## Stage 26 — Delivery diagnostics admin integration v1

Stage 26 adds a read-only admin UI integration for delivery diagnostics. The admin diagnostics menu can show a safe Russian DeliveryDiagnosticsSnapshot report built from repository delivery metrics. This stage does not change sender runtime, worker runtime, Telegram send gateway, rollout strategy activation, delivery attempts behavior, scheduler behavior, database schema, or migrations.


## Stage 27 runtime probe boundary

Stage 27 добавляет безопасную границу rollout для одиночного repost: `SenderService` получает optional `RepostSingleRolloutProbe`, `bot.py` создаёт его из env-config builder с disabled-by-default настройками. Probe не импортирует Telegram gateway/pipeline и не делает внешних write side effects; legacy sender продолжает выполнять фактический `copy_message`.
