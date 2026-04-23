# Systemd: runtime роли для production

Production-профиль: **split-runtime** с тремя сервисами:

- `forwarder-bot-ui.service` → `bot.py --role bot` (UI)
- `forwarder-bot-scheduler.service` → `bot.py --role scheduler`
- `forwarder-bot-worker.service` → `bot.py --role worker`

Legacy/dev fallback сохранён: `bot.py --role all`.

## Установка unit-файлов

```bash
sudo cp deploy/systemd/forwarder-bot-ui.service /etc/systemd/system/
sudo cp deploy/systemd/forwarder-bot-scheduler.service /etc/systemd/system/
sudo cp deploy/systemd/forwarder-bot-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
```

## Включение и запуск

```bash
sudo systemctl enable --now forwarder-bot-ui.service
sudo systemctl enable --now forwarder-bot-scheduler.service
sudo systemctl enable --now forwarder-bot-worker.service
```

## Проверка и диагностика

```bash
bash deploy/systemd/runtime-control.sh status
bash deploy/systemd/runtime-control.sh logs
bash deploy/systemd/runtime-control.sh smoke /opt/telegram-forwarder-bot
```

Или напрямую smoke-check:

```bash
bash deploy/systemd/smoke-check-runtime.sh /opt/telegram-forwarder-bot
```
