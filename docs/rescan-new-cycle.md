# Rescan: explicit new publication cycle

Incident #94: 181 logical pending posts became faulty (385 physical deliveries)
because historical ledger evidence was applied to a newly requested rescan.

Both modes pause the rule under the existing routing-row owner lock, retire
its delivery jobs (including previously failed jobs eligible for billing recovery),
mark interrupted processing deliveries as faulty, and cancel local worker tasks.
Remote workers lose their lease; their heartbeat checks every two seconds.
The parser does not wait for any Telegram send or old job to finish.

A revoked job cannot claim a new send attempt, enqueue a subsequent stage,
change delivery status or update the rule schedule. Publication preflight
rechecks its lease before each write using one read-only SQL statement, without
caching permission or retaining a database lock during network I/O. An RPC already issued to Telegram cannot
be recalled. A late remote publication may duplicate content in the explicitly
requested new cycle. Known self-loop echo evidence is retained.

The full source snapshot is required before atomic rebuild. Historical
attempts and receipts (including uncertain and modern intents) are archived;
unresolved receipt processing is retired, and original attempt outcomes and
MTProto identities remain available for audit. The replaced delivery rows are
recorded in audit_log. Existing confirmed complete logical posts are retained
in keep-position; incomplete albums are rebuilt as a whole, and newly
discovered older posts before the saved cursor are excluded rather than
rewinding it. Newer arrivals remain eligible. Rebuilt rows receive fresh
delivery IDs and therefore fresh publication keys. No old quarantine is applied.
A parser failure releases the rescan owner but leaves the rule paused; an
unknown database commit retains the owner for inspection.

Unrelated rules, existing reaction jobs and advertising cleanup are not cancelled.
Archived receipts do not block manual rollback or clearing the new queue;
archived attempts remain available even after a rollback of a retained sent item.
The production DB and server are not changed by this PR. Verify real PostgreSQL
regressions and all required reviews before merging.
