# Sender Architecture v2 — current baseline after Stage R1 cleanup

## Purpose

This document records the current safe architecture direction after the Sender Architecture Reset cleanup. It does not define an active replacement pipeline.

## Current production runtime

The production delivery flow remains:

```text
worker_runtime.py
→ SenderService
→ app/sender.py legacy delivery flow
→ wrapped sender.bot / wrapped sender.telethon
→ Telegram API / Telethon API
→ existing verification, reactions, DB finalization, audit and scheduling
```

`app/sender.py` remains the production source of truth. Legacy copy-first single repost behavior was not intentionally changed by Stage R1.

## Transport boundary

The existing transport layer remains in use and must be preserved:

```text
wrap_bot(... label="sender.bot")
wrap_telethon_client(... label="sender.telethon")
```

Future extraction work should keep using the existing transport layer rather than creating another transport abstraction.

## Reset decision

The experimental active canary / rollout / replacement pipeline branch is not part of production runtime. Stage R1 removed unused files and tests for that branch.

Do not reintroduce active single-repost pipeline, active canary, rollout replacement, gateway active send path, or separate target-verifier production delivery path without a new explicit architecture stage.

## Next step

The next step is legacy-preserving extraction: move existing working `sender.py` code into smaller modules without changing delivery behavior, queue behavior, video/album/campaign behavior, reaction behavior, transport behavior, schema, audit log or problem state.
