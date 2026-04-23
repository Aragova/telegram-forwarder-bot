# Deployment rollout (production)

Короткий безопасный сценарий обновления.

## 1) Обновить код

```bash
cd /opt/telegram-forwarder-bot
git fetch --all
git checkout <release-or-branch>
git pull --ff-only
```

## 2) Прогнать preflight для каждой роли

```bash
python3 bot.py --role bot --preflight-only
python3 bot.py --role scheduler --preflight-only
python3 bot.py --role worker --preflight-only
```

## 3) Перезапустить runtime роли

```bash
sudo systemctl restart forwarder-bot-ui.service
sudo systemctl restart forwarder-bot-scheduler.service
sudo systemctl restart forwarder-bot-worker.service
```

## 4) Smoke-check

```bash
bash deploy/systemd/smoke-check-runtime.sh /opt/telegram-forwarder-bot
```

## 5) Критерий успеха rollout

- все 3 сервиса `active`;
- `overall_status=healthy` в `--ops-status`;
- нет restart-loop по `NRestarts`;
- в логах нет критических startup ошибок.
