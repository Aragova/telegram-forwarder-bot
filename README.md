# Telegram Forwarder Bot

Продвинутый бот-пересыльщик на aiogram + Telethon.

## Что реализовано
- Каналы и группы с темами
- Источники и получатели
- Правила source -> target с индивидуальным интервалом
- Пауза / возобновление по отдельной связке
- Корректный старт нового правила: первое срабатывание не мгновенное, а через интервал
- Очередь доставки по каждому правилу отдельно
- Поддержка protected content через MTProto fallback:
  1. copy_message / copy_messages
  2. Telethon get_messages
  3. download_media + reupload
  4. text-only fallback
- Парсинг истории каналов и тем
- Детальная статистика и сброс очереди

## Почему архитектура изменена
В исходном коде поле `posts.sent` было общим для всех правил. Из-за этого один и тот же пост нельзя было безопасно доставлять в несколько получателей.
Теперь доставка вынесена в таблицу `deliveries`, и каждая связка source -> target имеет свою очередь.

## Запуск

### Production (рекомендуется): split-runtime

- UI: `python bot.py --role bot` (или `--role ui`)
- Scheduler: `python bot.py --role scheduler`
- Worker: `python bot.py --role worker`

Перед стартом роли:

```bash
python bot.py --role bot --preflight-only
python bot.py --role scheduler --preflight-only
python bot.py --role worker --preflight-only
```

Systemd и smoke-check: `docs/systemd-runtime-roles.md`.
Rollout: `docs/deployment-rollout.md`.
Rollback: `docs/deployment-rollback.md`.
Runbook: `docs/operations-runbook.md`.
Landing deployment: `docs/landing-deployment.md`.

### Legacy/dev fallback

`python bot.py` или `python bot.py --role all` — совместимый режим, но не основной production путь.

### Локальный старт (dev)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python bot.py
```

## Парсинг истории
```bash
python parse_history.py --source -1001234567890
python parse_history.py --source -1001234567890 --thread 123
python parse_history.py --source -1001234567890 --clean
```
