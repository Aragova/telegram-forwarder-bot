# Аудит bot.py после выноса VIP scheduled posts

## Цель
- `bot.py` по-прежнему остаётся интеграционным entrypoint: инициализация сервисов, регистрация `Dispatcher`-обработчиков, запуск runtime-задач.
- Текущая цель — продолжать уменьшать размер `bot.py` безопасными, изолированными выносами без изменения поведения в production.
- Этот документ является только аудитом и планом: runtime-поведение бота не меняется.

## Текущее состояние
- Примерный размер `bot.py`: **9748 строк** (`wc -l bot.py`).
- Примерное число регистраций `@dp.callback_query` в `bot.py`: **101** (`grep -n "@dp.callback_query" bot.py | wc -l`).
- Примерное число регистраций `@dp.message` в `bot.py`: **32** (`grep -n "@dp.message" bot.py | wc -l`).
- Уже вынесенные модули repost campaign:
  - `app/repost_campaign_handlers.py`
  - `app/repost_campaign_schedule_handlers.py`
  - `app/repost_campaign_report_handlers.py`
  - `app/repost_campaign_scheduled_post_handlers.py`
  - `app/repost_campaign_message_handlers.py`
  - `app/repost_campaign_context.py`
  - `app/repost_campaign_context_factory.py` — **в текущем дереве не найден**.
- VIP scheduled posts находятся вне `bot.py` (отдельный модуль `app/repost_campaign_scheduled_post_handlers.py`).

## Оставшиеся крупные зоны ответственности в bot.py

### 1) Bootstrap / инициализация / регистрация runtime
- Диапазон: примерно `1-2200`, плюс хвост запуска внизу файла.
- Маркеры: инициализация сервисов (`SubscriptionService`, `PaymentService`, `LimitService`), глобальные кэши (`rule_card_cache`, `rule_ui_tasks`), общие функции запуска/планировщика.
- Зависимости: `db`, `scheduler_service`, `subscription_service`, `payment_service`, `usage_service`, `tenant_service`, worker/runtime helpers.
- Почему ещё в `bot.py`: исторически это central composition root.
- Сложность выноса: **high**.
- Риск: **high**.
- Рекомендуемый модуль: `app/bot_bootstrap_runtime.py`.

### 2) Admin-only flows
- Диапазон: примерно `3163-3460` (live status/dashboard/rules list), плюс команды подтверждения платежей `3117-3141`.
- Префиксы/хендлеры: `dashboard_*`, `rules_*`, `/payment_confirm`, `/payment_reject`, `📈 Живой статус`, `📜 Список правил`.
- Зависимости: `scheduler_service`, `db`, admin access checks, billing events.
- Почему ещё в `bot.py`: тесная связка с текущими глобальными helper-функциями и общими клавиатурами.
- Сложность выноса: **medium**.
- Риск: **medium**.
- Рекомендуемый модуль: `app/admin_handlers.py`.

### 3) User SaaS/menu flows
- Диапазон: примерно `2140-3060`.
- Префиксы/хендлеры: `/start`, `/plans`, `/account`, `/usage`, `/subscription`, `user_*` (`user_main`, `user_channels`, `user_timezone:*`, `user_channel_*`, `user_status`, `user_account`, `user_plans`).
- Зависимости: `subscription_service`, `tenant_service`, `usage_service`, `payment_service`, `user_ui`, `user_states`.
- Почему ещё в `bot.py`: большой монолитный пользовательский сценарий, много shared utility.
- Сложность выноса: **medium**.
- Риск: **medium**.
- Рекомендуемый модуль: `app/user_menu_handlers.py`.

### 4) Остатки repost campaign в bot.py
- Диапазон: примерно `6383-6566`.
- Префиксы/хендлеры: `rule_repost_campaign_scheduled_detail`, `rule_repost_campaign_scheduled_cancel*`, `rule_repost_campaign_show_*`, `rule_repost_campaign_post_add`, `rule_repost_campaign_add_list`.
- Зависимости: `db`, repost-campaign services/context, `user_states`.
- Почему ещё в `bot.py`: legacy-остатки после частичного выноса.
- Сложность выноса: **low/medium**.
- Риск: **low/medium**.
- Рекомендуемый модуль: `app/repost_campaign_legacy_handlers.py`.

### 5) Video/editor flows
- Диапазон: примерно `5861-6842` + вспомогательные функции выше (`video_caption_*`, `video_intro_status_text`).
- Префиксы/хендлеры: `video_caption_*`, `set_video_caption_mode_*`, `caption_mode_menu:*`, `rule_video_clip_duration:*`.
- Зависимости: `db`, аудиты изменений правила, caption/intros UI helpers.
- Почему ещё в `bot.py`: пересечение с rule card и shared state.
- Сложность выноса: **medium/high**.
- Риск: **high**.
- Рекомендуемый модуль: `app/video_editor_handlers.py`.

### 6) Intro management flows
- Диапазон: примерно `5733-6300`.
- Префиксы/хендлеры: `video_intro_menu:*`, `intro_upload`, `intro_view:*`, `intro_delete:*`, `apply_intro:*`.
- Зависимости: файловая система (`FSInputFile`, пути), `db.get_intro/get_intros/delete_intro`, `user_states`.
- Почему ещё в `bot.py`: сочетание callback-flow + message-upload состояния.
- Сложность выноса: **medium**.
- Риск: **medium/high**.
- Рекомендуемый модуль: `app/intro_handlers.py`.

### 7) Rule card / rule management flows
- Диапазон: примерно `6302-7515`, плюс кэш/рендер helpers `315-600`.
- Префиксы/хендлеры: `rule_card:*`, `rule_refresh:*`, `enable_rule:*`, `disable_rule:*`, `toggle_rule_mode:*`, `delete_rule:*`, `rule_extra_menu:*`, `rule_to_list`.
- Зависимости: rule-card cache, scheduler, db, audit log, shared keyboard builders.
- Почему ещё в `bot.py`: центральный и самый связанный блок.
- Сложность выноса: **high**.
- Риск: **high**.
- Рекомендуемый модуль: `app/rule_management_handlers.py`.

### 8) Queue/manual send/rescan flows
- Диапазон: примерно `6917-7172` + job helpers `473-742`.
- Префиксы/хендлеры: `start_from_number:*`, `rollback:*`, `rescan_rule_*`, `trigger_now:*`, `rule_logs_*`.
- Зависимости: критичная логическая очередь, scheduler/runtime, rule refresh.
- Почему ещё в `bot.py`: высокая связанность с критичными инвариантами очереди.
- Сложность выноса: **high**.
- Риск: **high**.
- Рекомендуемый модуль: `app/rule_runtime_handlers.py`.

### 9) Payment/subscription flows
- Диапазон: примерно `927-1137` (helpers) и `2295-3141` (handlers).
- Префиксы/хендлеры: `/subscription`, `/billing`, `/invoice`, `product:*`, successful payment, manual confirm/reject.
- Зависимости: `subscription_service`, `payment_service`, payment intents в `db`, billing events.
- Почему ещё в `bot.py`: критичные бизнес-операции, частично смешаны с user menu.
- Сложность выноса: **medium/high**.
- Риск: **high**.
- Рекомендуемый модуль: `app/payment_handlers.py`.

### 10) Shared callback/message utilities
- Диапазон: примерно `813-1229` и другие utility-блоки.
- Маркеры: `ensure_rule_callback_access`, трансляция callback для user-mode карточек, helper построения меню.
- Зависимости: `access_control`, `tenant_service`, keyboard/UI utilities.
- Почему ещё в `bot.py`: используются почти везде.
- Сложность выноса: **medium**.
- Риск: **medium**.
- Рекомендуемый модуль: `app/bot_handler_utils.py`.

### 11) Background jobs/workers/scheduler/runtime glue
- Диапазон: кэш/таски `198-742`, фоновые job-coro и refresh glue, worker coordination.
- Маркеры: `rule_ui_tasks`, `_schedule_rule_ui_task`, `_run_rescan_rule_*`, `ensure_rule_workers` вызовы.
- Зависимости: scheduler/runtime services, asyncio tasks, db.
- Почему ещё в `bot.py`: orchestration-слой рядом с обработчиками.
- Сложность выноса: **high**.
- Риск: **high**.
- Рекомендуемый модуль: `app/runtime_glue.py`.

### 12) Error handling / access guards / callback answer helpers
- Диапазон: распределено по файлу, ключевые функции в early utility blocks.
- Маркеры: guard-проверки доступа к rule/user, safe answer/edit wrappers и их вызовы.
- Зависимости: aiogram callback/message API, access control, logger.
- Почему ещё в `bot.py`: общее использование и постепенная миграция в context-слой.
- Сложность выноса: **medium**.
- Риск: **medium**.
- Рекомендуемый модуль: `app/handler_guards.py`.

### 13) Legacy/дубли helper-логики
- Диапазон: разрозненно, особенно вокруг старых `user_*`/`rule_*` веток.
- Маркеры: отдельные однотипные keyboard/text routing helpers и совместимость callback-префиксов user/admin режимов.
- Зависимости: текущие callback схемы, UI builders.
- Почему ещё в `bot.py`: поддержка обратной совместимости.
- Сложность выноса: **medium**.
- Риск: **medium/high**.
- Рекомендуемый модуль: `app/legacy_handler_bridge.py`.

## Следующие кандидаты на вынос

1. **PR:** `Extract user menu/channel handlers from bot.py`
- Move only: `user_*` callbacks и текстовые user menu handlers (`2140-3060`, включая `user_channel_*`).
- Новый модуль: `app/user_menu_handlers.py`.
- Почему безопасно: callback-префиксы изолированы по `user_`, меньше пересечений с rule runtime.
- Ожидаемые файлы: `bot.py`, `app/user_menu_handlers.py`, возможно `app/user_menu_context.py`, точечные тесты UI.
- Тесты: существующие callback-prefix тесты + smoke на отсутствие дубликатов.
- Ручные проверки: `/start`, меню пользователя, добавление/удаление канала, смена языка/таймзоны.

2. **PR:** `Extract intro management handlers`
- Move only: `video_intro_menu`, `intro_upload/view/delete`, `apply_intro` блок (`5733-6300`).
- Новый модуль: `app/intro_handlers.py`.
- Почему безопасно: единый домен и явные callback-префиксы `intro_*`, `video_intro_*`, `apply_intro:*`.
- Ожидаемые файлы: `bot.py`, `app/intro_handlers.py`, возможно update shared context.
- Тесты: сценарии callback-маршрутов intro + compile tests.
- Ручные проверки: список заставок, загрузка файла, просмотр, удаление, назначение intro на правило.

3. **PR:** `Extract repost campaign legacy leftovers`
- Move only: остаточные `rule_repost_campaign_*` хендлеры в диапазоне `6383-6566`.
- Новый модуль: `app/repost_campaign_legacy_handlers.py`.
- Почему безопасно: небольшой объём и тематическая близость к уже вынесенным repost модулям.
- Ожидаемые файлы: `bot.py`, `app/repost_campaign_legacy_handlers.py`, `tests/test_repost_campaign_ui.py`.
- Тесты: текущий набор repost_campaign_ui + проверки, что VIP callbacks не возвращаются в `bot.py`.
- Ручные проверки: карточка кампании, show, add list, post add stub/cancel.

4. **PR:** `Extract admin dashboard/list handlers`
- Move only: `dashboard_*`, `rules_*`, `📈 Живой статус`, `📜 Список правил`.
- Новый модуль: `app/admin_dashboard_handlers.py`.
- Почему безопасно: связный админ-блок с ограниченным набором префиксов.
- Ожидаемые файлы: `bot.py`, `app/admin_dashboard_handlers.py`.
- Тесты: callback routing + команды админа.
- Ручные проверки: открытие dashboard, пауза/резюм, список правил и пагинация.

## Что пока не трогать
- Глубокий bootstrap/startup-shutdown orchestration (очень высокий риск регрессий запуска/фоновых задач).
- Scheduler/runtime glue и rescan/rollback/start-from-position ветки, где высок риск нарушить критическую логику очереди.
- Платежная активация (`successful_payment`, manual confirm/reject, intent activation) до отдельной изоляции сервисного слоя и расширенных regression tests.
- Переплетённые блоки video mode + rule card cache при отсутствии отдельного интеграционного набора тестов на видеопайплайн.

## Рекомендуемый следующий PR
- **PR title:** `Extract user menu and channel management handlers from bot.py`
- **Goal:** вынести пользовательский SaaS/menu блок (`user_*` callbacks + команды/кнопки пользовательского меню) в отдельный модуль без изменения поведения.
- **Move only:** диапазон `2140-3060` с префиксами `user_*`, `/start`-связанные пользовательские callbacks, user channels add/remove/state transitions.
- **Do not touch:** callback_data, UI тексты, state names, платежные обработчики, rule runtime, scheduler/worker glue.
- **Required tests:**
  - `python3 -m py_compile bot.py app/*.py tests/*.py scripts/*.py`
  - профильные user-menu/callback tests (добавить при необходимости)
  - существующие repost campaign UI tests как guard от побочных эффектов.
- **Manual checks:**
  - пользователь: `/start`, открытие `👤 Мой аккаунт`, `💎 Тарифы`, `🌐 Язык`, `📈 Использование`;
  - callbacks `user_channels`, `user_sources`, `user_targets`, add/remove channel flow;
  - cancel/state cleanup (`user_cancel`, `❌ Отмена`).


## A6a (2026-05-12): extracted read-only user menu navigation callbacks
- Extracted into `app/user_menu_handlers.py`: `user_main`, `user_channels`, `user_sources`, `user_targets`, `user_sources_list`, `user_targets_list`, `user_status`, `user_account`, `user_plans`.
- Left in `bot.py`: channel add/remove stateful flows (`user_sources_add`, `user_targets_add`, `user_channel_*remove*`, `user_channel_add_*`), user cancel flows (`user_cancel`, text cancel), `/start`, payment/subscription activation handlers.
- Approx metrics after A6a: bot.py line count and callback/message counts reduced (approximate, source-level extraction only).
- Recommended next PR: A6b — extract user channel management stateful handlers.
