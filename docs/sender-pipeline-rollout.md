# Sender Pipeline Rollout — archived after Stage R1 cleanup

## Current decision

Stage R1 cleanup removed the unused experimental active pipeline / rollout implementation from the repository. The production sender runtime is the legacy `app/sender.py` flow.

There is no active RepostSingle canary, no rollout replacement path, and no single-repost pipeline runtime wiring.

## Production source of truth

- `app/sender.py` legacy copy-first delivery remains the production source of truth for single reposts.
- `bot.py` does not instantiate active pipeline components.
- `sender.py` does not call active canary or pipeline code.
- Existing transport wrappers remain around `sender.bot` and `sender.telethon`.

## Next architecture direction

The next safe architecture work is extracting the existing working `sender.py` behavior into smaller modules without behavior changes. New work must not reintroduce the removed rollout/canary pipeline as an active runtime path.
