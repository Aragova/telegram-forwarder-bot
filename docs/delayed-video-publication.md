# Отложенная публикация видео: архитектурный аудит 24.11.2026

**DRAFT. Не мержить как исправление полного жизненного цикла.** Этот этап добавляет явное распознавание временного ID и запрет его ошибочной верификации; существующие вызывающие функции всё ещё завершают неразрешённый результат через manual review. Нужна отдельная долговечная ветка reconciliation, а не увеличение секунд ожидания.

## Production evidence (отчёт за 23–24.11)
- 13 failed jobs (5 MTProto unresolved, 2 album cache unresolved, 5 single cache unresolved, 1 attempted copy confirmation failure).
- 12 проблемных attempts, включая 1 исторический NameError `sent_message_ids` в #89.
- 25 текущих faulty deliveries, 14 уже очищены. 29 событий выбора транспорта по политике подписи — не сбои.
- #149: returned 2292/2293, pre-send watermark 5180/5182; #14: returned 1766, watermark 4003; #31: returned 388, watermark 1855. История в 50 сообщений не содержала новых кандидатов в коротком окне.
- 6 saved-post campaign target failures, raw album IDs есть, опубликованные ID не найдены.
- Признак video_processing_pending в исходной выгрузке не записан; гипотеза требует подтверждения. Не объявлять все эти попытки опубликованными.
- В отчёте нет подтверждения повреждения исходного MP4. Отдельно 2492 предупреждения input entity и 607 invalid reaction; числа относятся к строкам, не к уникальным сообщениям.

## Почему предыдущие исправления не решают эту ветку
#487/#488 искали ID в истории и ввели защиту от второго send. #507 усилил проверку peer/time/media, #508 разделил transport lanes и запретил ложный success, #509 сохранил ledger при перескане. #522/#523 сохранили receipts, запретили media-to-text и неоднозначный replay, исправили классификацию skips и ffprobe. Все эти меры нужны. Но поиск опубликованного ID не может надёжно найти ещё отложенное сообщение.

## Протокол Telegram
- https://core.telegram.org/api/scheduled-messages
- https://core.telegram.org/api/files
- https://core.telegram.org/constructor/updateDeleteScheduledMessages
- https://core.telegram.org/bots/api-changelog

MTProto: UpdateNewScheduledMessage / video_processing_pending — отдельное пространство ID; при публикации UpdateDeleteScheduledMessages содержит параллельные массивы messages и sent_messages. Не принимать удаление без sent_messages за публикацию. Bot API может вернуть message_id=0 с 01.12.2024; отсутствие ID не означает отклонение отправки. Клиенты Telegram Desktop и TDLib моделируют асинхронный переход между идентификаторами; см. https://github.com/telegramdesktop/tdesktop и https://github.com/tdlib/td.

## Обязательный следующий этап (единственный полноценный контур)
1. Общий контракт send receipt: immediate published, accepted pending publication, unknown, rejected before acceptance. Сохранить стабильный ключ попытки, аккаунт-отправитель, target peer/thread, scheduled IDs, время, размер альбома, необходимые метаданные для проверки. Не записывать временные ID в sent_message_ids_json и не считать их опубликованными. Для Bot API zero сохранять отдельный receipt без ложного ID.
2. Установить raw-update listener **до** отправки, на том же отправляющем клиенте. Обрабатывать exact pairs из UpdateDeleteScheduledMessages; валидировать длину, позитивность, уникальность, peer, session. Durable inbox для гонки update-before-receipt, повторов, restart и missed-update reconciliation.
3. Durable publication-pending state/queue и отдельный ограниченный worker чтения. Не занимать transport write или video slot ожиданием перекодировки. Не повторять исходный send из этой очереди. Проверить recovery при остановке и потерянном update; не использовать совпадение только подписи или последний пост.
4. После точного соответствия — проверка опубликованных сообщений (peer/topic, source guard, album completeness); атомарное сохранение final IDs + verified delivery + уникальная post-send задача. Ошибка post-send не должна возвращать доставку к отправке. Не трогать намеренные fixed повторы.
5. Для Bot API zero сопоставление channel_post только с однозначными независимыми доказательствами; при неоднозначности manual review, не угадывать. Не переключаться на второй транспорт после принятия.
6. SavedPostRenderer использует другой resolver; включить кампании, одиночные медиа, альбомы и обе транспортные ветки в тот же контракт. Не удалять старые кампании по временным ID.
7. Отдельная проверка actual corrupt source: размер скачивания, ffprobe video stream; ограниченное повторное скачивание, затем permanent invalid-source classification; не пересылать оторванную подпись. Ошибку ffprobe infrastructure не считать битым файлом.
8. Отдельно диагностировать доступность аккаунтов в целевом канале и допустимые реакции; это не должно менять результат доставки. Не заменять один аккаунт другим без tenant scope.

## Acceptance tests (до merge)
- Pending 20 секунд / 2 минуты / 15 минут, включая повторное обновление и рестарт.
- Два параллельных видео с одинаковой подписью; одинаковый scheduled ID в разных чатах/аккаунтах; нельзя принять старый published ID с тем же числом.
- Album: смешанный pending, частичное/переставленное событие, уникальность и порядок final IDs.
- Bot API ID=0, disconnect после send, update-before-receipt, no sent_messages при удалении, отсутствие update.
- Exact self-loop и разные темы, fixed повтор, повторная доставка одного worker job; никаких новых send на стадии reconciliation.
- Проверка старых #14/#31/#68/#89/#94/#149/#253/#254/#267 только read-only до независимого сопоставления. Не запускать массовый replay.
- После реализации: unit, integration PostgreSQL, CI/CodeQL/Sonar, тестовая публикация обычного несекретного видео в контролируемом канале и staged rollout.

**Текущий черновик не реализует пункты 1–6 полностью; изменение схемы/очереди требует отдельного согласованного архитектурного решения.** 

## Этап 2: точный MTProto inbox (черновик)
- Telegram Desktop scheduled_messages.cpp использует peer и соответствующие индексы двух массивов. TDLib updateMessageSendSucceeded заменяет временный ID, а не угадывает запись по истории. Telethon v1 messageparse.py явно предупреждает о коллизиях двух пространств ID; uploads.py создаёт random_id самостоятельно. Telegram docs https://core.telegram.org/api/updates отдельно описывают восстановление UpdateMessageID после потери RPC и дедупликацию.
- Добавлен PostgreSQL inbox с ключом (sender_account_id, target_peer_id, scheduled_id), проверкой полных массивов и блокировкой противоречивых соответствий. Raw listener устанавливается на реальный отправляющий аккаунт до запуска воркеров; повтор события не меняет доказательство. Не записываем текст, медиа или ключи сессии.
- Разрешатель одиночной отправки может использовать точный mapping, уже доставленный к моменту проверки, и проверяет peer, topic, watermark, from_scheduled, время и self-loop. Не подбирает публикацию по совпавшей подписи.
- **Не завершено:** автоматическая отложенная финализация поздних событий, durable receipt ↔ delivery_id, outbox последующих действий, альбомы/кампании/Bot API zero, сохранение random_id до низкоуровневого RPC, восстановление при сбое записи inbox. Telethon catch_up не делает запись PostgreSQL и сохранение pts одной транзакцией. Нельзя мержить или считать ночные сбои устранёнными.
- Применение схемы требует проверки на тестовой PostgreSQL, проверки индекса/роста inbox и политики хранения. Новый код слушателя пока не запускался на production.

Источники: https://core.telegram.org/api/scheduled-messages ; https://core.telegram.org/api/updates ; https://core.telegram.org/tdlib/getting-started ; https://github.com/telegramdesktop/tdesktop/blob/dev/Telegram/SourceFiles/data/components/scheduled_messages.cpp ; https://github.com/LonamiWebs/Telethon/blob/v1/telethon/client/messageparse.py ; https://github.com/LonamiWebs/Telethon/blob/v1/telethon/client/uploads.py ; https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html .
