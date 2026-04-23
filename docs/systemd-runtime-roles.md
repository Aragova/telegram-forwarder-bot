# Systemd deployment: separate runtime roles

Ниже — минимальный infrastructure-ready шаблон для раздельного запуска ролей:

- UI: `bot.py --role bot`
- Scheduler: `bot.py --role scheduler`
- Worker: `bot.py --role worker`

Готовые unit-файлы лежат в `deploy/systemd/`.

## 1) Установка unit-файлов

```bash
sudo cp deploy/systemd/forwarder-bot-ui.service /etc/systemd/system/
sudo cp deploy/systemd/forwarder-bot-scheduler.service /etc/systemd/system/
sudo cp deploy/systemd/forwarder-bot-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
```

## 2) Порядок запуска (безопасный rollout)

1. Запустить UI и убедиться, что админ-интерфейс доступен.
2. Запустить scheduler и проверить, что процесс живой.
3. Запустить worker и проверить, что обработки идут штатно.

```bash
sudo systemctl enable --now forwarder-bot-ui.service
sudo systemctl enable --now forwarder-bot-scheduler.service
sudo systemctl enable --now forwarder-bot-worker.service
```

## 3) Логи / status / restart

```bash
sudo systemctl status forwarder-bot-ui.service
sudo systemctl status forwarder-bot-scheduler.service
sudo systemctl status forwarder-bot-worker.service

sudo journalctl -u forwarder-bot-ui.service -f
sudo journalctl -u forwarder-bot-scheduler.service -f
sudo journalctl -u forwarder-bot-worker.service -f

sudo systemctl restart forwarder-bot-ui.service
sudo systemctl restart forwarder-bot-scheduler.service
sudo systemctl restart forwarder-bot-worker.service
```

## 4) Быстрый smoke-check после деплоя

```bash
bash deploy/systemd/smoke-check-roles.sh
```

## 5) Legacy fallback остаётся

Старый запуск совместим и не удаляется:

```bash
python3 bot.py
# или явный fallback
python3 bot.py --role all
```
