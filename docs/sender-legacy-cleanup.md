# Sender Legacy Cleanup Plan — reset after Stage R1 cleanup

## Current decision

Stage R1 cleanup keeps the legacy sender runtime as the production source of truth and removes the unused experimental active pipeline / rollout branch.

## What must remain stable

- single repost copy-first behavior;
- album delivery behavior;
- video delivery and ffmpeg processing behavior;
- campaign runtime behavior;
- reaction runtime behavior;
- queue, rollback and start-from-position behavior;
- transport wrappers around `sender.bot` and `sender.telethon`;
- repository schema, migrations, audit log and problem state unless a future task explicitly changes them.

## Cleanup result

The removed architecture was experimental-only and is not the rollout plan anymore. Tests for that architecture were removed with the unused modules.

## Next safe work

Future work should extract existing working `sender.py` code into modules without behavior change. Extraction must be small, test-backed and must not create a parallel delivery implementation.

## Compatibility notes

The old rollout vocabulary is retained only as historical context in tests that verify cleanup documentation exists; it is not an instruction to run shadow, active canary or fail-closed rollout modes. The current plan is extraction without behavior change, with rollback achieved by keeping existing `sender.py` behavior unchanged during each step. Existing `delivery_attempts` handling stays in `sender.py` until a separate safe extraction task moves it.
