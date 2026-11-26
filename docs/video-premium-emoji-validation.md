# Premium video caption validation

Incident: a Custom Emoji document was returned by Telegram and matched the UTF-16
substring, yet SendMedia rejected it with DOCUMENT_INVALID. Two occurrences of the
same ID failed independently. All other formatting worked.

## Operator setup

Create a dedicated private broadcast channel, without linked discussion or public
usernames. Grant the **same parser/Premium account used by the sender** permission
to publish and delete messages. Do not add the channel to any source, destination,
campaign, parser or third-party automation. Restrict membership to trusted operators.

Set in the service environment (or the project .env):

```dotenv
VIDEO_EMOJI_VALIDATION_CHAT_ID=-100...
```

The service host must have `ffmpeg` available in its `PATH` to generate the
synthetic probe clip. Check with `ffmpeg -version` in the service environment.

Restart the application normally. The validator refuses registered source/target
channels and active campaign destinations, and checks their isolation before generating a synthetic clip. The operator
must keep this channel isolated after setup; a concurrent external registration
cannot be made atomic with Telegram publication.

## Behavior

- New video captions with Custom Emoji must pass a real validation before saving.
  If the test channel is unconfigured, unavailable, busy, or the result is unknown,
  the previous caption stays unchanged. Ordinary captions and clearing still work.
- A dedicated button "Проверить Premium-эмодзи" checks an already stored video caption
  without editing it. The result is a separate message, not an edit of a potentially
  newer menu or input prompt. Existing captions are not silently rewritten or auto-scanned.
- Validate document IDs, UTF-16 substrings and Telegram Custom Emoji alt text (VS16-only differences are tested by the real probe).
- Use a one-second generated black MP4 with silent audio, and a random diagnostic
  marker plus only the ordinary emoji characters. No source video, original
  caption, links, usernames or user identifiers are sent to the test channel.
- Verify returned entities in the channel. Delete only an independently read
  message with the exact unique marker, peer and message ID.
- If the complete group is rejected, establish that plain media works, then
  isolate document IDs with bounded group probes; report all original occurrences.
  A failed control, timeout, FloodWait, missing response or probe budget exhaustion
  is inconclusive, never success. No further publication follows an unknown result.
- Limits: 50 occurrences, 12 probes per check, a 60-second live-check deadline.
  PostgreSQL reserves at most one check per account per 60 seconds and one per
  requesting user/account per 120 seconds across UI processes. Only one check
  may be active for an account; the existing transport cooldown table is reused.
  Account/peer FloodWait is respected and persisted. Existing cooldowns are
  checked before upload and immediately before the publication RPC. Diagnostic RPCs have no
  internal Telethon retries or flood sleeps; production task settings are unchanged.
  A queued upload or failed upload-part RPC cannot mark a publication uncertain.
  The uncertainty flag is set only immediately before the actual publication RPC;
  explicit rejection clears it, and successful send requires verified cleanup.
- An unknown send/verification/deletion result or process crash leaves a durable
  diagnostic-only fence (scope_key video-emoji:<account_id>:inflight). No new
  probe is allowed until the operator reconciles the isolated channel.
  Stop all diagnostic/UI processes, verify there are no outstanding probes,
  remove only verified synthetic messages if necessary, then delete **only**
  that exact fence row in telegram_transport_cooldowns. Do not delete normal
  Telegram cooldown rows or attempt to resolve an unknown send by sending again.
  Restart the UI normally. Never clear the fence while a check is running.
- A successful isolated probe is evidence for the parser account and test channel
  at that moment, not a lifetime guarantee for all target channels. Telegram may
  change availability or restrictions afterwards. Runtime publication retains its
  existing no-resend protection and now records Custom Emoji document IDs in the
  failure snapshot. No automatic fallback to unformatted text is introduced.
