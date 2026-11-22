# Rule 214: album ledger recovery (incident 2026-11-22)

## Confirmed incident
167 albums, three photos each. 105 albums have status sent. Of the remaining 62, 59 have old accepted attempt receipts and 3 have no receipts.
All 59 candidate albums have been read from Telegram and matched by count, source/target album grouping, media kind and caption. Photo bytes were not compared.

## Root cause
Before this patch both rescan modes deleted deliveries, then called parse_channel_history/parse_group_history(clean_start=True).
The parser deleted shared source posts, invalidating OTHER rules using the same source. Recreated delivery rows received new IDs,
while delivery_attempts kept prior IDs and stable album idempotency keys based on rule, target and media_group_id.
The sender correctly avoids a duplicate Telegram send but may spend an ordinary 8-hour scheduling slot reconciling each older attempt.

## Fix in this PR
KEEP POSITION: pause the rule after checking for active jobs, parse without deleting posts, update existing post content only when changed.
Preserve delivery IDs, sent/pending state and next_run_at; continue the existing queue.
EXPLICIT START OVER: parse without destructive source delete, archive prior delivery_attempts idempotency keys by appending a unique rescan suffix;
then delete only the selected rule's deliveries in the same transaction and rebuild. Old receipts remain in PostgreSQL for audit.
Explicit start over intentionally replays old posts and can create duplicate channel content. It must not be used as a repair shortcut.
Do not rescan if any active jobs or processing deliveries exist. If a destructive reset fails after committing, leave the rule paused.
Two nullable routing columns (rescan_owner_token, rescan_started_at) and a default-false rescan_queue_built flag are added idempotently on database initialization. No changes to reaction sessions.

## Safe repair of existing rule 214
Take a PostgreSQL backup before any --apply. Commands run from the existing server using its authorized Telethon session:

Read-only:
    sudo -u michael140887 -H bash -c 'cd /home/michael140887/project && ./venv/bin/python scripts/reconcile_rule_album_ledger.py --rule-id 214'

Apply after inspecting the dry run and accepting structure-only verification:
    sudo -u michael140887 -H bash -c 'cd /home/michael140887/project && ./venv/bin/python scripts/reconcile_rule_album_ledger.py --rule-id 214 --apply --accept-structural-match'

The script copies the existing session, never logs in or sends Telegram messages, and reads target/source batches with FloodWait handling.
All selected IDs must exist with the expected album group, media kinds and exact captions. Topic targets are manual-review only.
Each confirmed album undergoes fenced atomic reconciliation: lock routing, attempt and deliveries; check no active jobs;
revalidate target/channel, idempotency key, stored IDs, source IDs and all pending statuses; update the ledger to verified and all three
deliveries to sent in one transaction. If any check fails, no rows for that album are committed. Next_run_at is unchanged.

## Follow-up
Expected if there is no concurrent progress: 164 sent albums (492 deliveries) and 3 pending albums (9 deliveries).
Those final three have no saved attempt and must be handled by the normal eight-hour scheduler.
Historical sent rows without destination message IDs are not modified. The script does NOT compare photo contents byte-for-byte.
Never perform a global queue reset to recover this incident.

## Review #509 hardening: multi-worker fencing and deleted messages

- Both rescans acquire a routing row owner token under FOR UPDATE before the
  history scan and retain it until the whole parse/backfill/restore is complete.
  Another rescan on another UI screen or worker is rejected, even if the first
  temporarily disabled the rule. Finish and replay reset both require the
  original owner token. No single-process-only asyncio lock is assumed.
- The parser collects message IDs actually seen in the *completed* Telegram
  scan. Backfill filters to these IDs; old posts cached in PostgreSQL but deleted
  from Telegram cannot silently become fresh replay deliveries.
- Keep-position also removes stale **pending** deliveries for messages absent
  from Telegram, while retaining all sent history. It preserves the explicit
  start-position boundary independently of the deleted-message filter.
- A failed per-message parse refuses to use incomplete observed-ID data for
  pruning or replay.
- If the bot process dies in the middle of a rescan, the persisted owner token
  is intentionally not auto-released. The affected rule remains paused instead
  of risking duplicate sends. Diagnose the state and active jobs before
  manually clearing a stale owner, then explicitly resume the rule. Never
  clear an owner token during a live scan.

### Operator inspection after interrupted rescan (read only)

    SELECT id, is_active, rescan_owner_token, rescan_started_at
    FROM routing WHERE id = 214;

An owner token on an inactive rule is a safety latch, **not** permission to
reset deliveries. Verify the original process is no longer running and that
there are no active jobs, then obtain a DB snapshot before any manual recovery.

## Review #509 follow-up: activation after a completed scan

Enabling an inactive rule previously ran unfiltered backfill against all cached
posts. That would recreate deliveries for source messages removed from Telegram,
even after a correctly filtered "start over" or "keep position" rescan.

The selected rule now records `rescan_queue_built = TRUE` in the SAME owner-
fenced transaction that finishes a successful rescan. Future enable/activation
reuses its already rebuilt queue rather than scanning unrelated cached posts.
Rules that have never completed a rescan keep their existing initial backfill
behavior. New source posts continue to get deliveries from the ordinary parser
path. Manual activation remains blocked while a rescan owner holds the rule.

## Review #509 follow-up: protect live arrivals during history rescans

A source message can arrive after Telegram's reverse history iterator finishes,
while the normal real-time handler is still saving new posts into PostgreSQL.
A naive "not in observed set" filter would delete that valid pending delivery,
or fail to reconstruct it during a fresh replay.

After acquiring the per-rule owner, rescans capture the highest cached source
Telegram message ID **before** the historical scan. Telegram channel message IDs
increase monotonically. Only rows at or below this snapshot high-water mark
may be classified stale based on absence from the iterator. The replay backfill
always includes posts above this boundary even if they were not present in the
historical scan result. Rows that arrive after the iteration but before the
rebuild therefore remain eligible. All of this happens without deleting shared
source posts or loosening the idempotency checks on previously sent albums.
