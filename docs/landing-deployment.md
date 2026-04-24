# Landing deployment

## Где лежит landing

Статический сайт расположен в директории `landing/`:

- `landing/index.html`
- `landing/styles.css`
- `landing/app.js`
- `landing/ru/*.html`
- `landing/en/*.html`

## Где менять placeholders

Откройте `landing/app.js` и обновите объект `SITE_CONFIG`:

- `botUrl` — ссылка на Telegram-бота;
- `supportUrl` — Telegram поддержки;
- `supportEmail` — email поддержки;
- `legalOwner` — юридическое имя/название владельца.

Также при необходимости замените placeholder-ссылки в `landing/ru/*.html` и `landing/en/*.html`.

## GitHub Pages

1. Запушьте ветку в GitHub.
2. В `Settings → Pages` выберите Source: `Deploy from a branch`.
3. Выберите ветку и папку `/(root)`.
4. Убедитесь, что `landing/` доступен по адресу сайта (например, через `/landing/`).
5. При необходимости перенесите содержимое `landing/` в корень отдельного pages-репозитория.

## Nginx

Пример конфига:

```nginx
server {
    listen 80;
    server_name example.com;

    root /var/www/channelpilot/landing;
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;
    }
}
```

После изменения конфига выполните `nginx -t` и перезагрузите Nginx.

## Cloudflare Pages

1. Создайте проект Pages и подключите GitHub-репозиторий.
2. Build command: пусто.
3. Build output directory: `landing`.
4. Деплойте и проверьте роуты `/ru/*` и `/en/*`.

## Что проверить перед подключением платёжных провайдеров

1. Ссылки `botUrl/supportUrl/supportEmail/legalOwner` заполнены реальными значениями.
2. На главной есть честная формулировка про auto/manual payment confirmation.
3. Legal-страницы доступны на RU/EN и содержат disclaimer шаблона.
4. На сайте нет обещаний о 100% автоматической обработке всех платежей.
5. Перед продакшен-публикацией тексты Terms/Privacy/Refund проверены юристом.
