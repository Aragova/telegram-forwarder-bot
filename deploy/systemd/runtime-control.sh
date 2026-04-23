#!/usr/bin/env bash
set -euo pipefail

SERVICES=(
  forwarder-bot-ui.service
  forwarder-bot-scheduler.service
  forwarder-bot-worker.service
)

cmd="${1:-status}"

case "$cmd" in
  status)
    for svc in "${SERVICES[@]}"; do
      echo "== $svc =="
      systemctl --no-pager --full status "$svc" | sed -n '1,12p' || true
      echo
    done
    ;;
  logs)
    for svc in "${SERVICES[@]}"; do
      echo "== $svc (последние 40 строк) =="
      journalctl -u "$svc" -n 40 --no-pager || true
      echo
    done
    ;;
  smoke)
    "$(dirname "$0")/smoke-check-runtime.sh" "${2:-/opt/telegram-forwarder-bot}"
    ;;
  *)
    echo "Использование: $0 {status|logs|smoke [repo_path]}"
    exit 2
    ;;
esac
