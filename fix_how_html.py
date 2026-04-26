from pathlib import Path

p = Path("landing/index.html")
s = p.read_text()

start = s.index('<section class="how-section" id="how">')
end = s.index('<section id="pricing"', start)

new = '''<section class="how-section" id="how">
      <div class="how-container">
        <div class="how-head">
          <div>
            <div class="eyebrow">
              <span></span>
              <span data-i18n="how_title">Как это работает</span>
            </div>

            <h1 data-i18n="how_main_title">ViMi берёт канал под автопилот за 5 шагов</h1>
          </div>

          <p class="lead" data-i18n="how_main_lead">
            Подключаете источник и получателя, выбираете режим работы, задаёте расписание — дальше бот сам ведёт очередь, видео, подписи и публикации.
          </p>
        </div>

        <div class="how-grid">
          <div class="steps">
            <article class="step-card">
              <div class="step-num">1</div>
              <div class="step-text">
                <h3 data-i18n="step_1_t">Откройте бота</h3>
                <p data-i18n="step_1_d">Запустите ViMi в Telegram за пару секунд.</p>
              </div>
              <div class="step-chip">⚡</div>
            </article>

            <article class="step-card">
              <div class="step-num">2</div>
              <div class="step-text">
                <h3 data-i18n="step_2_t">Добавьте источники и каналы</h3>
                <p data-i18n="step_2_d">Укажите откуда брать контент и куда публиковать.</p>
              </div>
              <div class="step-chip">📡</div>
            </article>

            <article class="step-card">
              <div class="step-num">3</div>
              <div class="step-text">
                <h3 data-i18n="step_3_t">Создайте правило публикации</h3>
                <p data-i18n="step_3_d">Настройте расписание и параметры публикаций.</p>
              </div>
              <div class="step-chip">🕒</div>
            </article>

            <article class="step-card">
              <div class="step-num">4</div>
              <div class="step-text">
                <h3 data-i18n="step_4_t">Подключите тариф или оплату</h3>
                <p data-i18n="step_4_d">Выберите тариф и оплатите удобным способом.</p>
              </div>
              <div class="step-chip">💳</div>
            </article>

            <article class="step-card">
              <div class="step-num">5</div>
              <div class="step-text">
                <h3 data-i18n="step_5_t">Бот выполняет рутину за вас</h3>
                <p data-i18n="step_5_d">ViMi работает 24/7, вы экономите время и растите быстрее.</p>
              </div>
              <div class="step-chip">✅</div>
            </article>
          </div>

          <aside class="demo-card" aria-label="Пример интерфейса ViMi">
            <div class="phone-top">
              <div class="bot-title">
                <div class="bot-avatar">Vi</div>
                <div>
                  <div>ViMi</div>
                  <div class="online" data-i18n="demo_online">работает 24/7</div>
                </div>
              </div>
              <div class="live-chip">Live</div>
            </div>

            <div class="message">
              <strong data-i18n="demo_rule_title">Правило публикации</strong>
              <span data-i18n="demo_rule_text">Источник → Канал получатель · Интервал: 30 мин</span>
            </div>

            <div class="message">
              <strong data-i18n="demo_video_title">Видео обработано</strong>
              <span data-i18n="demo_video_text">Заставка добавлена, подпись сохранена, публикация в очереди.</span>
            </div>

            <div class="status-box">
              <div class="status-row"><span data-i18n="demo_status">Статус</span><b data-i18n="demo_status_value">Работает</b></div>
              <div class="status-row"><span data-i18n="demo_queue">В очереди</span><b data-i18n="demo_queue_value">12 публикаций</b></div>
              <div class="status-row"><span data-i18n="demo_next">Следующий пост</span><b>20:25</b></div>
            </div>
          </aside>
        </div>
      </div>
    </section>

    '''

p.write_text(s[:start] + new + s[end:])
