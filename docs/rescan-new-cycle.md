# Rescan: explicit new publication cycle

Incident #94: 181 logical pending posts became faulty (385 physical deliveries)
because historical ledger evidence was applied to a newly requested rescan.

This branch is a **draft**, not deployable. First increment: use one queue
rebuild for both modes; retain sent anchors in keep-position, archive historical
attempts/receipts and replaced deliveries, create new pending delivery IDs,
preserve the existing full-source snapshot and self-loop checks.

## Required before merge

- Revoke only the selected rule's active delivery jobs at rescan start; leave
  unrelated campaign cleanup and other rules intact.
- Add a persistent generation/lease fence at attempt creation, every transport
  dispatch, downstream stage enqueue, receipt finalization and schedule updates.
  Local cancellation cannot recall a Telegram RPC already issued.
- Prevent obsolete jobs from resurrecting via unfenced retry/fail operations.
- Define fail-closed behavior after cancellation and a failed/incomplete scan.
- Adapt old regression assertions that required historical quarantine or veto;
  test both modes, active workers and late callbacks against real PostgreSQL.
- Verify mixed/partially published albums, deleted source messages and new
  arrivals, positional semantics, audit storage cost and historical reaction handling.
- Run required CI, Codex, Gemini and SonarCloud on the final implementation.

No production repair or deployment is authorized by this draft.
