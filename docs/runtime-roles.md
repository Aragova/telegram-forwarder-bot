# Runtime roles

## Основной production путь

Используется **split-runtime**:

1. `bot` (UI роль) — Telegram UI/polling и операторские команды.
2. `scheduler` — постановка/планирование jobs.
3. `worker` — выполнение jobs и video pipeline.

## Совместимый fallback / dev режим

`all` — legacy-режим, в котором роли совмещены в одном процессе.

> Для production рекомендуется только split-runtime под systemd.

## Контракт запуска

- Поддерживаемые роли: `bot`, `ui` (алиас к `bot`), `scheduler`, `worker`, `all`.
- `bot/ui` и `all` требуют полный Telegram/env набор.
- `scheduler` требует только БД/env для scheduler-контура.
- `worker` требует БД + Telegram client env.
- Перед реальным стартом выполняется preflight (`run_preflight_checks`).
