#!/usr/bin/env bash
set -euo pipefail

services=(
  forwarder-bot-ui.service
  forwarder-bot-scheduler.service
  forwarder-bot-worker.service
)

for svc in "${services[@]}"; do
  echo "==> checking ${svc}"
  systemctl is-enabled "${svc}" || true
  systemctl is-active "${svc}" || true
  systemctl --no-pager --full status "${svc}" | sed -n '1,12p'
  echo
  journalctl -u "${svc}" -n 20 --no-pager
  echo
  echo "----------------------------------------"
  echo
 done
