# Stage 3 — campaign publication safety (#525)

This stage extends the existing durable transport and exact MTProto inbox, not a new sender. An unresolved write is never retried automatically. A zero Bot API ID is not a confirmed publication. For premium albums the complete exact vector and peer/topic must be verified before deletion. The reconciliation window is 24 hours; after it expires, evidence remains for operator review.

## Incident runbook

1. Inspect `campaign_run_messages`, `publication_review_required`, `publication_receipt` and `publication_raw_ids`. Do not use a raw scheduled ID as a published ID.
2. Stop campaign workers and pause the affected rule before applying an operator acknowledgment. Inspect the actual target, forum topic, scheduled messages, and all members of the album. If the outcome is uncertain, keep the placement blocked.
3. `scripts/acknowledge_interrupted_campaign_send.py MESSAGE_ID` is read-only by default. Apply only after independently confirming there is no live or scheduled placement, using the exact timestamp from the inspection.
4. `scripts/acknowledge_publication_receipt.py KEY` acknowledges an expired ordinary delivery receipt without replaying or inventing IDs.
5. `scripts/acknowledge_publication_inbox_incident.py ACCOUNT_ID` displays durable overflow evidence. Keep the write gate closed while reviewing the affected account. Acknowledge offline only after inspection and accepting that any unresolved receipts must stay quarantined. Restart the workers afterwards. The read-only difference recovery may recover exact pairs, but cannot guarantee that Telegram returns every lost event.

## Production acceptance (manual, isolated test destination only)

- Send a text ad and a media ad via Bot API and Premium. Check the exact target/topic, accepted IDs, run status, display time and deletion.
- Send a Premium video that Telegram processes asynchronously. Check that raw scheduled IDs never trigger deletion and the timer begins at actual publication.
- Test a complete album including mixed pending/immediate IDs. Confirm all members before deleting any.
- Restart after RPC acceptance and before reconciliation. Confirm one publication, recovered IDs, timer, Top-time, views and eventual deletion.
- Interrupt a request and confirm that a potentially accepted placement is fenced, while an explicit rate limit remains retryable for campaign copies.
- Review and acknowledge a deliberately expired test receipt without replay.
- Do not induce overflow or interrupt real production campaigns to perform these tests. Use a disposable account/environment for incident injection.

CI uses simulated Telegram responses and real disposable PostgreSQL. These tests cannot certify a live Telegram session.
