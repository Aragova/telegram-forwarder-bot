# Чистый канал для рекламных кампаний

## Что это

“Чистый канал” помогает не публиковать новую базовую рекламу поверх активной базовой рекламы.

Важно:

* относится к базовым режимам:
  * ⚡ Запустить сейчас
  * 🕒 Запланировать запуск
* не относится к VIP “🕒 Запланированные посты”
* не является рекламным кабинетом или CRM

## Ручной запуск

Clean Channel ON:

* active placement → блокировка;
* clean → запуск.

Clean Channel OFF:

* active placement → предупреждение;
* подтверждение → запуск поверх активной рекламы.

## Запланированный запуск

При создании расписания:

* policy строится заранее;
* preview показывает предупреждение или notice;
* confirm повторно проверяет policy.

В момент старта:

* clean → launch;
* ON + busy → waiting_clean_channel;
* OFF + busy → launch over active;
* base_block → failed;
* policy_error → needs_review.

## waiting_clean_channel

Статус означает:

* запуск не потерян;
* реклама не опубликована поверх активной;
* worker повторит проверку позже.

Поля:

* clean_channel_next_retry_at
* clean_channel_wait_attempt_count
* clean_channel_last_wait_at
* clean_channel_last_reason
* clean_channel_policy_json

UI показывает:

* следующую проверку;
* попытки ожидания;
* безопасную причину;
* кнопку активных размещений;
* отмену запуска.

## Границы

Manual force:

* использует force_ignore_clean_channel;
* не используется scheduled worker.

Scheduled overlap:

* использует ignore_active_placement_block только для run_type="scheduled" и только когда Clean Channel выключен;
* action фиксируется как schedule_with_overlap_warning.

Scheduled wait:

* при включённом Clean Channel и занятом канале worker сохраняет waiting_clean_channel;
* action фиксируется как schedule_with_clean_channel_wait;
* повторная проверка выполняется по clean_channel_next_retry_at.

VIP scheduled posts:

* отдельный сценарий;
* не подключён к ordinary waiting_clean_channel.

## Этапы реализации

* 6.1 — read-only scheduled policy state
* 6.2 — UI preview/warning
* 6.3 — handlers wiring
* 6.4 — repository waiting/retry state
* 6.5 — due-worker enforcement
* 6.6 — list/detail UI polish
* 6.7 — smoke/source audit + docs

## Smoke checklist

* manual flow работает отдельно;
* scheduled flow строит policy при создании;
* scheduled worker ждёт чистый канал при ON + busy;
* scheduled worker запускает поверх active only при OFF + busy;
* waiting_clean_channel виден в списке и деталях;
* VIP scheduled posts не затронуты.
