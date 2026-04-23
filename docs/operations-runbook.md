# Operations runbook

## Базовые команды

```bash
sudo systemctl start forwarder-bot-ui.service forwarder-bot-scheduler.service forwarder-bot-worker.service
sudo systemctl stop forwarder-bot-worker.service forwarder-bot-scheduler.service forwarder-bot-ui.service
sudo systemctl restart forwarder-bot-ui.service forwarder-bot-scheduler.service forwarder-bot-worker.service
bash deploy/systemd/runtime-control.sh status
bash deploy/systemd/runtime-control.sh logs
python3 bot.py --ops-status
```

## Если роль не стартует

Проверьте:
1. `EnvironmentFile` и обязательные env.
2. `WorkingDirectory` и наличие `bot.py`.
3. preflight: `python3 bot.py --role <role> --preflight-only`.

## Если heartbeat красный

- `python3 bot.py --ops-status` → `role_problems`.
- `journalctl -u forwarder-bot-<role>.service -n 80 --no-pager`.

## Если saturated mode

- Проверьте backlog в `--ops-status`.
- Убедитесь, что `worker` активен и не в restart loop.

## Если heavy backlog растёт

- Проверить `worker` логи и video stage jobs.
- Проверить доступ к `media/temp/intros`.

## Если worker restart loop

- Проверить `systemctl show forwarder-bot-worker.service --property=NRestarts`.
- Проверить env Telegram/БД и preflight worker.

## Если scheduler не ставит jobs

- Проверить активность `forwarder-bot-scheduler.service`.
- Проверить heartbeat scheduler в `--ops-status`.

## Если video pipeline зависает

- Проверить логи worker.
- Проверить место/доступ к temp/media.
- Проверить `system_mode` и нагрузку backlog.
