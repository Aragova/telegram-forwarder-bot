#!/usr/bin/env bash
set -euo pipefail

SERVICES=(
  forwarder-bot-ui.service
  forwarder-bot-scheduler.service
  forwarder-bot-worker.service
)

ROOT_DIR="${1:-/opt/telegram-forwarder-bot}"

failures=0

echo "[SMOKE] Проверка systemd-ролей"
for svc in "${SERVICES[@]}"; do
  enabled="$(systemctl is-enabled "$svc" 2>/dev/null || true)"
  active="$(systemctl is-active "$svc" 2>/dev/null || true)"
  restarts="$(systemctl show "$svc" --property=NRestarts --value 2>/dev/null || echo 0)"

  echo "- $svc: enabled=$enabled active=$active restarts=$restarts"

  if [[ "$active" != "active" ]]; then
    echo "  Подсказка: сервис не активен. Проверьте EnvironmentFile, WorkingDirectory и preflight.";
    failures=$((failures+1))
  fi

  if [[ "$restarts" =~ ^[0-9]+$ ]] && (( restarts > 5 )); then
    echo "  Подсказка: похоже на restart loop. Проверьте journalctl -u $svc -n 80"
    failures=$((failures+1))
  fi
done

echo
echo "[SMOKE] Проверка operational health"
if ! ops_json="$(cd "$ROOT_DIR" && /usr/bin/python3 bot.py --ops-status --json 2>/tmp/forwarder_ops_err.log)"; then
  echo "Не удалось получить operational status."
  cat /tmp/forwarder_ops_err.log || true
  failures=$((failures+1))
else
  echo "$ops_json"
  if ! grep -q '"overall_status": "healthy"' <<<"$ops_json"; then
    echo "  Подсказка: статус не healthy. Проверьте roles/system_mode/backlog в выводе выше."
    failures=$((failures+1))
  fi
fi

if (( failures > 0 )); then
  echo
  echo "[SMOKE] Обнаружены проблемы ($failures). Короткая диагностика:"
  for svc in "${SERVICES[@]}"; do
    echo "----- $svc (последние 20 строк) -----"
    journalctl -u "$svc" -n 20 --no-pager || true
  done
  exit 1
fi

echo "[SMOKE] OK: runtime выглядит здоровым"
