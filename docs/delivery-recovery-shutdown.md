# P0.8/P0.9: восстановление и завершение доставок

База: main @ 61879df662e6b68a8dcf3eb6669c878a663c0a99.
См. [предыдущий аудит](delivery_reliability_audit_2026-11-23.md),
[graceful drain](delivery_reliability_phase2.md), [новый цикл пересканирования](rescan-new-cycle.md)
и [runtime roles](runtime-roles.md).

## Контракт

- Запуск любой runtime-роли не меняет delivery processing на pending.
- Job сохраняет payload, ID, dedup key и историю. Просроченный lease и
  устаревший heartbeat восстанавливаются существующим watchdog, а не
  глобальным изменением статусов доставок.
- Незапущенный lease возвращается в retry только его текущим владельцем,
  без списания попытки при defer. Просроченный lease не может начать RPC.
- При завершении worker loop сохраняет владение дочерними задачами до
  их завершения даже при повторной отмене. Отмена не означает отказ Telegram
  и не разрешает новую публикацию.
- Не меняем схему PostgreSQL, ledger/receipts, правила fixed и self-loop,
  алгоритм очереди, видеопайплайн, политику транспорта и реакции.

## Матрица восстановления

| Состояние | Действие |
| --- | --- |
| Живой processing job | Не трогать при старте другой роли |
| Leased, не начат | Owner-fenced defer или истечение lease |
| Stale processing job | Существующий watchdog, затем проверка ledger/receipt в sender |
| Verified | Использовать существующий verified cache, не повторять RPC |
| Accepted / sending / unknown | Существующая сверка или ручная проверка, без слепого fallback |
| MTProto scheduled receipt | Существующий reconciler, сохранить receipt |
| Нет активного job | Не считать отсутствие job доказательством отсутствия отправки |

## Деплой и проверка

Не считать этот draft готовым к merge до реальных PostgreSQL-тестов,
проверки orphan deliveries без jobs и ручного reset, проверки обоих
транспортов, альбомов, rescan, fixed и self-loop. Отдельно проверить
systemd override на реально работающем сервере:
`systemctl show forwarder_bot.service -p TimeoutStopUSec -p KillSignal`.
Ожидается 720 секунд и SIGINT. Git pull не устанавливает override.

Ограничение: отсутствие активного job само по себе не доказывает результат
Telegram. Автоматический ремонт исторических orphan deliveries и
администраторский ручной reset требуют отдельной проверки в рамках этого PR.
