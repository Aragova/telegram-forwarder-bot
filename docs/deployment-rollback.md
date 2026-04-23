# Deployment rollback (production)

Откат без импровизации и без потери состояния jobs.

## 1) Зафиксировать проблемный rollout

```bash
bash deploy/systemd/runtime-control.sh status
bash deploy/systemd/runtime-control.sh logs
```

## 2) Остановить роли

```bash
sudo systemctl stop forwarder-bot-worker.service
sudo systemctl stop forwarder-bot-scheduler.service
sudo systemctl stop forwarder-bot-ui.service
```

## 3) Вернуть предыдущий стабильный коммит

```bash
cd /opt/telegram-forwarder-bot
git fetch --all
git checkout <stable_commit_or_tag>
```

## 4) Preflight после отката

```bash
python3 bot.py --role bot --preflight-only
python3 bot.py --role scheduler --preflight-only
python3 bot.py --role worker --preflight-only
```

## 5) Поднять роли обратно

```bash
sudo systemctl start forwarder-bot-ui.service
sudo systemctl start forwarder-bot-scheduler.service
sudo systemctl start forwarder-bot-worker.service
```

## 6) Проверить health после rollback

```bash
python3 bot.py --ops-status
bash deploy/systemd/smoke-check-runtime.sh /opt/telegram-forwarder-bot
```

## Важно по состоянию jobs/deliveries

- схема БД на этом этапе не меняется;
- rollback выполняется только по коду/юнитам;
- при частично начатом rollout сначала остановить все роли, затем откатывать код.
