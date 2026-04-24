const SITE_CONFIG = {
  botUrl: "https://t.me/YOUR_BOT_USERNAME",
  supportUrl: "https://t.me/YOUR_SUPPORT_USERNAME",
  supportEmail: "support@vimi.bot",
};

const CONTENT = {
  ru: {
    navFeatures: "Возможности",
    navPlans: "Тарифы",
    navHow: "Инструкции",
    navFaq: "FAQ",
    navSupport: "Поддержка",
    openBot: "Открыть бота",
    viewPlans: "Смотреть тарифы",
    heroBadge: "Ваш помощник в Telegram",
    heroTitle: "Автоматизация Telegram-каналов в одном боте",
    heroSubtitle: "Публикации, видео, тарифы, счета и приём оплаты — в современной SaaS-платформе ViMi.",
    floatAuto: "Автопостинг",
    floatVideo: "Видео",
    floatAnalytics: "Статистика",
    floatMonet: "Монетизация",
    trustLabel: "Поддерживаем платежи",
    featuresTitle: "Что умеет ViMi",
    features: [
      ["📣", "Автопостинг", "Публикует записи из источников по расписанию."],
      ["🎬", "Видео", "Очередь, обложки и подготовка видео-контента."],
      ["💎", "Тарифы", "Гибкие планы под объём вашего канала."],
      ["💳", "Оплата", "Счета и оплата прямо внутри Telegram."],
      ["📊", "Статистика", "Метрики активности и отчёты по публикациям."],
      ["⚙️", "Автоматизация", "Меньше рутины и больше времени на развитие."],
    ],
    howTitle: "Как это работает",
    steps: [
      ["Открыть бота", "Запустите ViMi в Telegram.", "assets/vimi-step-robot-open.png"],
      ["Добавить каналы", "Подключите источники и целевые каналы.", "assets/vimi-robot-head.png"],
      ["Настроить правила", "Определите формат и расписание публикации.", "assets/vimi-hero-robot.png"],
      ["Подключить тариф", "Выберите подходящий план и оплату.", "assets/vimi-pricing-cards.png"],
      ["Бот работает", "ViMi запускает поток задач автоматически.", "assets/vimi-step-robot-done.png"],
    ],
    plansTitle: "Тарифы",
    plans: [
      { name: "FREE", price: "0 USD / месяц", points: ["3 правила", "5 видео / день", "100 задач / день"] },
      { name: "BASIC", price: "9 USD / месяц", points: ["15 правил", "30 видео / день", "1000 задач / день"], highlight: true },
      { name: "PRO", price: "29 USD / месяц", points: ["50 правил", "100 видео / день", "5000 задач / день"] },
    ],
    popular: "Популярный",
    pickPlan: "Выбрать в боте",
    paymentsTitle: "Способы оплаты",
    payments: ["Telegram", "PayPal", "Карты", "Крипта", "СБП", "Tribute / Lava"],
    manualNote: "⚠️ Некоторые методы работают вручную",
    finalTitle: "Готовы автоматизировать ваши каналы?",
    finalSubtitle: "Откройте ViMi в Telegram и начните уже сегодня.",
    footerDesc: "ViMi — premium Telegram SaaS для роста и автоматизации каналов.",
    faq: [
      ["Как быстро запустить?", "Откройте бота, добавьте каналы и включите первое правило."],
      ["Можно ли работать с видео?", "Да, доступны подготовка видео и публикация по расписанию."],
      ["Какие варианты оплаты?", "Telegram, PayPal, карты, крипта, СБП, Tribute/Lava."],
    ],
  },
  en: {
    navFeatures: "Features",
    navPlans: "Pricing",
    navHow: "How it works",
    navFaq: "FAQ",
    navSupport: "Support",
    openBot: "Open bot",
    viewPlans: "View pricing",
    heroBadge: "Your Telegram assistant",
    heroTitle: "Automate Telegram channels in one bot",
    heroSubtitle: "Publishing, video processing, plans, invoices and payments in modern ViMi SaaS.",
    floatAuto: "Autoposting",
    floatVideo: "Video",
    floatAnalytics: "Analytics",
    floatMonet: "Monetization",
    trustLabel: "Supported payments",
    featuresTitle: "What ViMi can do",
    features: [
      ["📣", "Autoposting", "Schedules channel posts from your sources."],
      ["🎬", "Video", "Queue, covers and video content preparation."],
      ["💎", "Pricing", "Flexible plans for your channel scale."],
      ["💳", "Payments", "Invoices and payments directly in Telegram."],
      ["📊", "Statistics", "Live metrics and publication reports."],
      ["⚙️", "Automation", "Less routine, more growth time."],
    ],
    howTitle: "How it works",
    steps: [
      ["Open the bot", "Launch ViMi in Telegram.", "assets/vimi-step-robot-open.png"],
      ["Add channels", "Connect sources and destination channels.", "assets/vimi-robot-head.png"],
      ["Set rules", "Configure publication format and schedule.", "assets/vimi-hero-robot.png"],
      ["Pick plan", "Select the best plan and payment method.", "assets/vimi-pricing-cards.png"],
      ["Bot runs", "ViMi handles routine operations automatically.", "assets/vimi-step-robot-done.png"],
    ],
    plansTitle: "Pricing",
    plans: [
      { name: "FREE", price: "0 USD / month", points: ["3 rules", "5 videos / day", "100 tasks / day"] },
      { name: "BASIC", price: "9 USD / month", points: ["15 rules", "30 videos / day", "1000 tasks / day"], highlight: true },
      { name: "PRO", price: "29 USD / month", points: ["50 rules", "100 videos / day", "5000 tasks / day"] },
    ],
    popular: "Popular",
    pickPlan: "Choose in bot",
    paymentsTitle: "Payment methods",
    payments: ["Telegram", "PayPal", "Cards", "Crypto", "SBP", "Tribute / Lava"],
    manualNote: "⚠️ Some methods require manual processing",
    finalTitle: "Ready to automate your channels?",
    finalSubtitle: "Open ViMi in Telegram and launch today.",
    footerDesc: "ViMi is a premium Telegram SaaS for channel growth and automation.",
    faq: [
      ["How fast can I start?", "Open bot, add channels and launch your first rule."],
      ["Can I process videos?", "Yes, video preparation and scheduled publishing are supported."],
      ["What payment options exist?", "Telegram, PayPal, cards, crypto, SBP, Tribute/Lava."],
    ],
  }
};

function fillI18n(lang) {
  document.documentElement.lang = lang;
  document.querySelectorAll("[data-i18n]").forEach((node) => {
    const key = node.dataset.i18n;
    if (CONTENT[lang][key]) {
      node.textContent = CONTENT[lang][key];
    }
  });
}

function fillFeatures(lang) {
  const grid = document.querySelector("#featuresGrid");
  grid.innerHTML = "";
  CONTENT[lang].features.forEach((item) => {
    const card = document.createElement("article");
    card.className = "feature-card reveal";
    card.innerHTML = `<div class="feature-ico">${item[0]}</div><h3>${item[1]}</h3><p>${item[2]}</p>`;
    grid.appendChild(card);
  });
}

function fillSteps(lang) {
  const grid = document.querySelector("#stepsGrid");
  grid.innerHTML = "";
  CONTENT[lang].steps.forEach((step, index) => {
    const card = document.createElement("article");
    card.className = "step-card reveal";
    card.innerHTML = `<div class="step-index">${index + 1}</div><img src="${step[2]}" alt="${step[0]}" loading="lazy"/><h3>${step[0]}</h3><p>${step[1]}</p>`;
    grid.appendChild(card);
  });
}

function fillPlans(lang) {
  const grid = document.querySelector("#plansGrid");
  grid.innerHTML = "";
  CONTENT[lang].plans.forEach((plan) => {
    const card = document.createElement("article");
    card.className = `plan-card reveal ${plan.highlight ? "highlight" : ""}`.trim();
    card.innerHTML = `
      ${plan.highlight ? `<span class="plan-pill">${CONTENT[lang].popular}</span>` : ""}
      <h3>${plan.name}</h3>
      <div class="plan-price">${plan.price}</div>
      <ul>${plan.points.map((point) => `<li>${point}</li>`).join("")}</ul>
      <a class="btn btn-ghost" href="${SITE_CONFIG.botUrl}" target="_blank" rel="noopener">${CONTENT[lang].pickPlan}</a>
    `;
    grid.appendChild(card);
  });
}

function fillPayments(lang) {
  const grid = document.querySelector("#paymentsGrid");
  grid.innerHTML = "";
  CONTENT[lang].payments.forEach((method) => {
    const node = document.createElement("div");
    node.className = "pay-item";
    node.textContent = method;
    grid.appendChild(node);
  });
}

function fillFaq(lang) {
  const list = document.querySelector("#faqList");
  list.innerHTML = "";
  CONTENT[lang].faq.forEach((entry) => {
    const item = document.createElement("article");
    item.className = "faq-item";
    item.innerHTML = `<button class="faq-q" type="button">${entry[0]}</button><div class="faq-a"><p>${entry[1]}</p></div>`;
    list.appendChild(item);
  });
  list.querySelectorAll(".faq-q").forEach((button) => {
    button.addEventListener("click", () => {
      button.closest(".faq-item").classList.toggle("open");
    });
  });
}

function applyLinks(lang) {
  ["header-open-bot", "hero-open-bot", "footer-open-bot"].forEach((id) => {
    const link = document.getElementById(id);
    if (link) {
      link.href = SITE_CONFIG.botUrl;
    }
  });

  const supportLink = document.getElementById("supportLink");
  const supportEmail = document.getElementById("supportEmail");
  supportLink.href = SITE_CONFIG.supportUrl;
  supportEmail.href = `mailto:${SITE_CONFIG.supportEmail}`;
  supportEmail.textContent = SITE_CONFIG.supportEmail;

  document.getElementById("link-terms").href = `${lang}/terms.html`;
  document.getElementById("link-privacy").href = `${lang}/privacy.html`;
  document.getElementById("link-refund").href = `${lang}/refund.html`;
}

function revealOnScroll() {
  const observer = new IntersectionObserver((entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        entry.target.classList.add("on");
      }
    });
  }, { threshold: 0.2 });

  document.querySelectorAll(".reveal").forEach((node) => observer.observe(node));
}

function setLanguage(lang) {
  fillI18n(lang);
  fillFeatures(lang);
  fillSteps(lang);
  fillPlans(lang);
  fillPayments(lang);
  fillFaq(lang);
  applyLinks(lang);

  document.querySelectorAll(".lang-btn").forEach((button) => {
    button.classList.toggle("active", button.dataset.lang === lang);
  });

  localStorage.setItem("vimi-lang", lang);
  requestAnimationFrame(revealOnScroll);
}

function setupUI() {
  const toggle = document.getElementById("menuToggle");
  const nav = document.getElementById("mainNav");

  toggle.addEventListener("click", () => {
    nav.classList.toggle("open");
  });

  document.querySelectorAll(".lang-btn").forEach((button) => {
    button.addEventListener("click", () => setLanguage(button.dataset.lang));
  });
}

setupUI();
setLanguage(localStorage.getItem("vimi-lang") || "ru");
