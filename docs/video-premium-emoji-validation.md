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
synthetic probe clip, with the `libx264` and AAC encoders. Run this local smoke
test in the service environment; it does not contact Telegram:

```bash
(
  set -e
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  ffmpeg -nostdin -hide_banner -v error \
    -f lavfi -i color=c=black:s=64x64:r=30 \
    -f lavfi -i anullsrc=r=44100:cl=stereo \
    -t 1 -c:v libx264 -preset ultrafast -pix_fmt yuv420p \
    -c:a aac -threads 1 -movflags +faststart -y "$tmp/probe.mp4"
  test -s "$tmp/probe.mp4"
)
```

Restart the application normally. The validator refuses registered source/target
channels and active campaign destinations, and checks their isolation before generating a synthetic clip. The operator
must keep this channel isolated after setup; a concurrent external registration
cannot be made atomic with Telegram publication.

## Behavior

- New video captions with Custom Emoji must pass a real validation before saving.
  If the test channel is unconfigured, unavailable, busy, or the result is unknown,
  the previous caption stays unchanged. Ordinary captions and clearing still work.
  Within the UI process, a newly accepted input or clear invalidates older
  pending input for the same rule, even if the new input fails validation.
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
  isolate exact (document ID, symbol) variants with bounded group probes; report
  original occurrences of each rejected variant.
  A failed control, timeout, FloodWait, missing response or probe budget exhaustion
  is inconclusive, never success. No new application-level probe follows an unknown result.
- Limits: 50 occurrences, 12 probes per check, a 60-second live-check deadline.
  PostgreSQL reserves at most one check per account per 60 seconds and one per
  requesting user/account per 120 seconds across UI processes. Only one check
  may be active for an account; the existing transport cooldown table is reused.
  Account/peer FloodWait is respected and persisted. Existing cooldowns are
  checked before upload and immediately before the publication RPC. Diagnostic RPCs have no
  application-level Telethon request retries or flood sleeps; production task settings are unchanged.
  MTProto auto-reconnect can requeue the same unanswered request; a UI timeout
  does not establish that a publication can no longer appear.
  A queued upload or failed upload-part RPC cannot mark a publication uncertain.
  The uncertainty flag is set only immediately before the actual publication RPC;
  explicit rejection clears it, and successful send requires verified cleanup.
- An unknown send/verification/deletion result or process crash leaves a durable
  diagnostic-only fence (scope_key video-emoji:<account_id>:inflight). No new
  probe is allowed until the operator reconciles the isolated channel.
  Stop all diagnostic/UI processes and disconnect their MTProto clients
  (including shared sender clients); do not merely cancel the UI handler.
  Verify there are no outstanding probes,
  remove only verified synthetic messages if necessary, then delete **only**
  that exact fence row in telegram_transport_cooldowns. Do not delete normal
  Telegram cooldown rows or attempt to resolve an unknown send by sending again.
  Restart the UI normally. Never clear the fence while a check is running.
- A successful isolated probe is evidence for the parser account and test channel
  at that moment, not a lifetime guarantee for all target channels. Telegram may
  change availability or restrictions afterwards. Runtime publication retains its
  existing no-resend protection and now records Custom Emoji document IDs in the
  failure snapshot. No automatic fallback to unformatted text is introduced.
