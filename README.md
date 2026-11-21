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


## Architecture notes

- [Sender Architecture v2](docs/sender-architecture-v2.md)

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


## ViMi | Support (@vimi_support_bot)

Этот бот **не является частью основного процесса** `bot.py`: его
исходник находится отдельно в `support_bot.py` и запускается
`python support_bot.py` с переменными окружения
`SUPPORT_BOT_TOKEN` и `SUPPORT_ADMIN_ID` (не хранить секреты в Git).

- `/start` (включая ссылки из ViMi Pay), `/help` и `/menu` показывают
  подсказку, **не пересылаются оператору** и не создают обращений.
- Все служебные сообщения бота, включая приветствие, ошибки, подтверждения и
  инструкции оператору, имеют версии **RU/EN**: язык Telegram `ru`
  (включая `ru-RU`) получает русский интерфейс; остальные и
  неопределённые языки получают английский. Пользовательские вопросы и
  ответы оператора автоматически не переводятся.
- Остальные команды с `/` в начале также не пересылаются.
- Обычные сообщения и вложения пересылаются оператору; он отвечает в
  Telegram через Reply на пересланное обращение.
- Соответствие пересланных сообщений адресатам пока хранится **в памяти
  процесса**. После рестарта ответ на ранее пересланное сообщение
  может не доставиться. Не считать это восстановленной перепиской.

**Где запущен:** этот репозиторий не содержит service/unit для
`support_bot.py`, поэтому путь и способ автозапуска действующего
бота нельзя подтвердить по GitHub. Для проверки на предполагаемой VM
используйте только read-only команды (не вставляйте в чат содержимое
`.env`):

```bash
ps -eo pid,user,args | grep '[s]upport_bot.py'
systemctl list-units --all --type=service --no-pager | grep -Ei 'support|vimi'
docker ps --format '{{.Names}} {{.Image}} {{.Status}}' | grep -Ei 'support|vimi'
```

Обновление `support_bot.py` потребует перезапуска **именно его
процесса/сервиса** после отдельной проверки фактического деплоя.
Перезапуск `forwarder-bot-ui`, scheduler и worker ради поддержки
не нужен. Workflow для статического сайта также не обновляет
поддержку.
