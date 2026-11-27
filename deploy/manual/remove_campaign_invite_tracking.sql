-- Run only after all old processes are stopped, a backup is verified,
-- and the new release is deployed. Does not revoke Telegram links.
BEGIN;
SET LOCAL lock_timeout = '5s';
DROP TABLE IF EXISTS campaign_invite_link_events;
DROP TABLE IF EXISTS campaign_invite_links;
DROP TABLE IF EXISTS campaign_invite_link_settings;
COMMIT;
