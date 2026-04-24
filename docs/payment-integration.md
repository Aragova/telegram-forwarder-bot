# Payment Integration Layer

## Поддержанные провайдеры

- `telegram_stars` — Telegram Stars (через Telegram update flow).
- `telegram_payments` — Telegram Payments (через pre-checkout/successful payment updates).
- `paypal` — sandbox-ready adapter, checkout URL + webhook contract.
- `tribute` — webhook-first adapter с проверкой подписи.
- `lava_top` — безопасный skeleton adapter + webhook contract.
- `manual_bank_card` / `card_provider` — ручной перевод по инструкции.
- `sbp_provider` — ручной СБП-перевод по инструкции.
- `crypto_manual` — ручная/semi-auto оплата (USDT TRC20 / USDT TON / BTC).

## Автоматические vs manual

- Автоматические/условно-автоматические: Telegram, PayPal, Tribute, Lava.top (после подключения реальных credentials/API).
- Manual: bank/card, СБП, crypto manual — всегда требуют подтверждения админом.

## Конфигурация ENV

### Общие

- `PAYMENT_ENABLED`
- `PAYMENT_DEFAULT_PROVIDER`
- `PAYMENT_ALLOWED_PROVIDERS`

### PayPal

- `PAYPAL_ENABLED`
- `PAYPAL_CLIENT_ID`
- `PAYPAL_CLIENT_SECRET`
- `PAYPAL_ENV`
- `PAYPAL_WEBHOOK_ID`

### Telegram

- `TELEGRAM_STARS_ENABLED`
- `TELEGRAM_PAYMENTS_ENABLED`
- `TELEGRAM_PAYMENT_PROVIDER_TOKEN`

### Manual card/bank

- `MANUAL_CARD_ENABLED`
- `MANUAL_CARD_TEXT_RU`
- `MANUAL_CARD_TEXT_EN`

### SBP

- `SBP_MANUAL_ENABLED`
- `SBP_PAYMENT_TEXT_RU`
- `SBP_PAYMENT_TEXT_EN`

### Crypto

- `CRYPTO_MANUAL_ENABLED`
- `CRYPTO_USDT_TRC20_ADDRESS`
- `CRYPTO_USDT_TON_ADDRESS`
- `CRYPTO_BTC_ADDRESS`

### Tribute

- `TRIBUTE_ENABLED`
- `TRIBUTE_API_KEY`
- `TRIBUTE_WEBHOOK_SECRET`

### Lava.top

- `LAVA_TOP_ENABLED`
- `LAVA_TOP_API_KEY`
- `LAVA_TOP_WEBHOOK_SECRET`

## Важные ограничения

- Telegram Stars подходят только для цифровых услуг внутри Telegram.
- PayPal требует реальные credentials и webhook endpoint для production.
- Bank/card/SBP/crypto manual не считаются оплаченными автоматически.
- Tribute/Lava.top требуют реальные API/webhook credentials для полного автомата.

## Webhook integration

На текущем этапе подготовлен service-level webhook слой (`app/payment_webhook_service.py`).
Отдельный HTTP сервер не поднимается: endpoint-обвязка должна быть подключена через будущий web-gateway.
