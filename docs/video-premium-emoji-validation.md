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

Restart the application normally. The validator refuses registered source/target
channels and checks their isolation before generating a synthetic clip. The operator
must keep this channel isolated after setup; a concurrent external registration
cannot be made atomic with Telegram publication.

## Behavior

- New video captions with Custom Emoji must pass a real validation before saving.
  If the test channel is unconfigured, unavailable, busy, or the result is unknown,
  the previous caption stays unchanged. Ordinary captions and clearing still work.
- A dedicated button "Проверить Premium-эмодзи" checks an already stored video caption
  without editing it. Existing captions are not silently rewritten or auto-scanned.
- Validate document IDs, exact UTF-16 substrings and Telegram Custom Emoji alt text.
- Use a one-second generated black MP4 with silent audio, and a random diagnostic
  marker plus only the ordinary emoji characters. No source video, original
  caption, links, usernames or user identifiers are sent to the test channel.
- Verify returned entities in the channel. Delete only an independently read
  message with the exact unique marker, peer and message ID.
- If the complete group is rejected, establish that plain media works, then
  isolate document IDs with bounded group probes; report all original occurrences.
  A failed control, timeout, FloodWait, missing response or probe budget exhaustion
  is inconclusive, never success. No further publication follows an unknown result.
- Limits: 50 occurrences, 12 probes, 60 seconds total, one check at a time per UI
  process. FloodWait extends the local validation cooldown. The sender transport
  wrapper still applies its own policies.
- If a response or cleanup fails, a synthetic diagnostic message may remain.
  Operators may remove it manually. Do not attempt to resolve an unknown send by
  sending another copy.
- A successful isolated probe is evidence for the parser account and test channel
  at that moment, not a lifetime guarantee for all target channels. Telegram may
  change availability or restrictions afterwards. Runtime publication retains its
  existing no-resend protection and now records Custom Emoji document IDs in the
  failure snapshot. No automatic fallback to unformatted text is introduced.
