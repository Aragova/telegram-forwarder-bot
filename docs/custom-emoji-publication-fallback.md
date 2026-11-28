# Custom emoji publication fallback

A Telegram custom emoji is a formatting entity over a normal Unicode emoji.
Telegram can reject SendMedia with DOCUMENT_INVALID even when the emoji
document is still discoverable (rule 169, 2026-11-28). Reuploading the same
media with the same caption does not solve this.

For a fenced MTProto production publication only, a definite
DocumentInvalidError on the exact SendMessage, SendMedia or SendMultiMedia
request permits one immediate retry. Remove custom emoji entities from that
request, retaining the underlying Unicode text, all other formatting,
media handles, routing and album membership. An otherwise successful request
retains every custom emoji. When Telegram does not identify the offending
entity, all custom emoji in the rejected request are downgraded; this is a
deliberate availability-first policy, not an assertion that every emoji is bad.

The retry re-enters shared cooldown, inbox readiness, durable random_id
identity and job fencing. Changed captions use the next ordinal. No retry
is allowed on a timeout, unrelated RPC, uncertain send, or a rejected
request with no custom emoji. No recursive retry. Confirmation of the actual
publication and existing delivery handling remain unchanged.

Diagnostic probes deliberately do not downgrade emojis: they must observe
the real rejection. The validator recognizes InvokeWithoutUpdatesRequest
wrapping SendMedia. Existing explicit UI validation remains unchanged.
Bot API copy and true forward operations have no editable caption entities
at this boundary and retain their existing behavior. If a stripped request
still fails, normal media error policy applies. No database migration.

Regression coverage: single media and album, wrapped and direct rejection,
preserved text/media/formatting, durable identity, non-recursion, no emoji,
unrelated RPC, unknown result, diagnostic isolation.
