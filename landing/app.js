const SITE_CONFIG = {
  botUrl: "https://t.me/YOUR_BOT_USERNAME",
  supportUrl: "https://t.me/YOUR_SUPPORT_USERNAME",
  supportEmail: "support@example.com",
};

const CONTENT = {
  ru: {
    positioning: "ViMi — ваш робот-помощник для Telegram-каналов",
    heroTitle: "ViMi — автоматизация Telegram-каналов в одном боте",
    heroSubtitle: "Публикации, видео, тарифы, счета и оплата — всё внутри Telegram. ViMi берёт рутину на себя, чтобы вы развивали канал быстрее.",
    openBot: "🚀 Открыть бота",
    viewPlans: "💎 Смотреть тарифы",
    navFeatures: "Возможности",
    navHow: "Как работает",
    navPlans: "Тарифы",
    navPayments: "Оплата",
    navFaq: "FAQ",
    floatAuto: "Автопостинг",
    floatVideo: "Видео",
    floatPlans: "Тарифы",
    floatPayments: "Оплата",
    trustStrip: "Поддерживаем: Telegram Payments, PayPal, банковские карты, СБП, криптовалюту, Tribute, Lava.top и ручное подтверждение",
    featuresTitle: "Что умеет ViMi",
    features: [
      ["📨", "Автопостинг", "Публикации по расписанию в каналы без ручной рутины."],
      ["🎬", "Видео", "Обработка видео, обложки и подписи в удобном сценарии."],
      ["💎", "Тарифы", "Гибкие планы под рост каналов и команд."],
      ["💳", "Счета и оплата", "Создание счетов и прием оплат прямо в Telegram."],
      ["📊", "Статистика", "Понятная аналитика активности и результатов."],
      ["⚙️", "Автоматизация", "Больше времени на контент, меньше повторяющихся действий."],
    ],
    howTitle: "Как это работает",
    steps: [
      "Откройте бота",
      "Добавьте источники и каналы",
      "Создайте правило публикации",
      "Подключите тариф или оплату",
      "ViMi выполняет рутину за вас",
    ],
    plansTitle: "Тарифы",
    plans: [
      { name: "FREE", price: "0 USD / месяц", limits: ["3 правила", "5 видео / день", "100 задач / день"] },
      { name: "BASIC", price: "9 USD / месяц", limits: ["15 правил", "30 видео / день", "1000 задач / день"], popular: true },
      { name: "PRO", price: "29 USD / месяц", limits: ["50 правил", "100 видео / день", "5000 задач / день"] },
    ],
    choosePlan: "Выбрать в боте",
    popular: "Популярный",
    paymentsTitle: "Способы оплаты",
    payments: [
      "Telegram Payments / Stars",
      "PayPal",
      "Банковские карты",
      "СБП",
      "Криптовалюта",
      "Tribute",
      "Lava.top",
      "Ручное подтверждение",
    ],
    paymentsDisclaimer: "Некоторые способы оплаты работают автоматически, а некоторые могут требовать ручного подтверждения администратором.",
    faqTitle: "FAQ",
    faq: [
      ["Что такое ViMi?", "ViMi — это Telegram-бот для автоматизации публикаций, видео и платежных сценариев."],
      ["Это сайт или Telegram-бот?", "Сайт показывает возможности продукта, а вся основная работа происходит в Telegram-боте ViMi."],
      ["Можно ли использовать несколько каналов?", "Да, количество подключаемых каналов зависит от выбранного тарифа."],
      ["Что происходит при превышении лимитов?", "Бот сообщит о лимитах и предложит перейти на тариф с более высоким объёмом."],
      ["Как работает оплата?", "Вы создаёте счёт внутри бота и оплачиваете удобным способом."],
      ["Можно ли вернуть деньги?", "Запрос на возврат рассматривается по условиям страницы Refund."],
      ["Как связаться с поддержкой?", "Через Telegram и email в разделе контактов."],
    ],
    finalCtaTitle: "Готовы автоматизировать ваши Telegram-каналы?",
    finalCtaSubtitle: "Откройте ViMi в Telegram и настройте первый сценарий за несколько минут.",
    footerDesc: "ViMi — ваш робот-помощник для Telegram-каналов.",
    footerProduct: "Продукт",
    footerDocs: "Документы",
    footerSupport: "Поддержка",
    legalLinks: ["Terms", "Privacy", "Refund", "Contacts", "Instructions"],
  },
  en: {
    positioning: "ViMi — your robot assistant for Telegram channels",
    heroTitle: "ViMi — Telegram channel automation in one bot",
    heroSubtitle: "Publishing, video processing, plans, invoices and payments — all inside Telegram. ViMi handles routine work so you can grow your channels faster.",
    openBot: "🚀 Open bot",
    viewPlans: "💎 View plans",
    navFeatures: "Features",
    navHow: "How it works",
    navPlans: "Plans",
    navPayments: "Payments",
    navFaq: "FAQ",
    floatAuto: "Autopublishing",
    floatVideo: "Video",
    floatPlans: "Plans",
    floatPayments: "Payments",
    trustStrip: "Supported: Telegram Payments, PayPal, bank cards, fast payments, crypto, Tribute, Lava.top and manual confirmation",
    featuresTitle: "What ViMi can do",
    features: [
      ["📨", "Autopublishing", "Scheduled publishing to channels without manual routine."],
      ["🎬", "Video", "Video processing, covers and captions in one workflow."],
      ["💎", "Plans", "Flexible plans for growing channels and teams."],
      ["💳", "Invoices and payments", "Create invoices and accept payments directly in Telegram."],
      ["📊", "Statistics", "Clear analytics of activity and results."],
      ["⚙️", "Automation", "Less repetitive work and more time for growth."],
    ],
    howTitle: "How it works",
    steps: [
      "Open the bot",
      "Add sources and channels",
      "Create a publishing rule",
      "Connect a plan or payment",
      "ViMi handles routine for you",
    ],
    plansTitle: "Plans",
    plans: [
      { name: "FREE", price: "0 USD / month", limits: ["3 rules", "5 videos / day", "100 tasks / day"] },
      { name: "BASIC", price: "9 USD / month", limits: ["15 rules", "30 videos / day", "1000 tasks / day"], popular: true },
      { name: "PRO", price: "29 USD / month", limits: ["50 rules", "100 videos / day", "5000 tasks / day"] },
    ],
    choosePlan: "Choose in bot",
    popular: "Popular",
    paymentsTitle: "Payment methods",
    payments: [
      "Telegram Payments / Stars",
      "PayPal",
      "Bank cards",
      "Fast payments",
      "Crypto",
      "Tribute",
      "Lava.top",
      "Manual confirmation",
    ],
    paymentsDisclaimer: "Some payment methods work automatically, while others may require manual administrator confirmation.",
    faqTitle: "FAQ",
    faq: [
      ["What is ViMi?", "ViMi is a Telegram bot for automating publishing, video processing and payment workflows."],
      ["Is it a website or a Telegram bot?", "The website presents the product, while all core actions run inside the ViMi Telegram bot."],
      ["Can I use multiple channels?", "Yes, the number of connected channels depends on your plan."],
      ["What happens when limits are exceeded?", "The bot notifies you about limits and offers a higher plan."],
      ["How does payment work?", "You create an invoice inside the bot and pay with a supported method."],
      ["Can I get a refund?", "Refund requests are reviewed under the Refund page terms."],
      ["How can I contact support?", "Use Telegram and email links in the contacts section."],
    ],
    finalCtaTitle: "Ready to automate your Telegram channels?",
    finalCtaSubtitle: "Open ViMi in Telegram and configure your first scenario in minutes.",
    footerDesc: "ViMi — your robot assistant for Telegram channels.",
    footerProduct: "Product",
    footerDocs: "Documents",
    footerSupport: "Support",
    legalLinks: ["Terms", "Privacy", "Refund", "Contacts", "Instructions"],
  }
};

function fillFeatures(items) {
  const grid = document.querySelector("#featuresGrid");
  grid.innerHTML = "";
  items.forEach((item) => {
    const card = document.createElement("article");
    card.className = "feature-card";
    card.innerHTML = `<div class="feature-icon">${item[0]}</div><h3>${item[1]}</h3><p>${item[2]}</p>`;
    grid.appendChild(card);
  });
}

function fillSteps(items) {
  const grid = document.querySelector("#stepsGrid");
  grid.innerHTML = "";
  items.forEach((text, idx) => {
    const card = document.createElement("article");
    card.className = "step-card";
    card.innerHTML = `<span class="step-number">${idx + 1}</span><h3>🤖 ${text}</h3>`;
    grid.appendChild(card);
  });
}

function fillPlans(plans, lang) {
  const grid = document.querySelector("#plansGrid");
  grid.innerHTML = "";
  plans.forEach((plan) => {
    const card = document.createElement("article");
    card.className = `plan-card${plan.popular ? " popular" : ""}`;
    card.innerHTML = `
      ${plan.popular ? `<span class="popular-badge">${CONTENT[lang].popular}</span>` : ""}
      <h3>${plan.name}</h3>
      <div class="price">${plan.price}</div>
      <ul>${plan.limits.map((x) => `<li>${x}</li>`).join("")}</ul>
      <a class="btn btn-secondary" href="${SITE_CONFIG.botUrl}" target="_blank" rel="noopener">${CONTENT[lang].choosePlan}</a>`;
    grid.appendChild(card);
  });
}

function fillPayments(items) {
  const grid = document.querySelector("#paymentsGrid");
  grid.innerHTML = "";
  items.forEach((item) => {
    const node = document.createElement("div");
    node.className = "pay-item";
    node.textContent = item;
    grid.appendChild(node);
  });
}

function fillFaq(items) {
  const node = document.querySelector("#faqList");
  node.innerHTML = "";
  items.forEach((entry) => {
    const item = document.createElement("details");
    item.className = "faq-item";
    item.innerHTML = `<summary><strong>${entry[0]}</strong></summary><p>${entry[1]}</p>`;
    node.appendChild(item);
  });
}

function setLanguage(lang) {
  const t = CONTENT[lang];
  document.documentElement.lang = lang;
  document.querySelectorAll("[data-i18n]").forEach((node) => {
    node.textContent = t[node.dataset.i18n];
  });

  fillFeatures(t.features);
  fillSteps(t.steps);
  fillPlans(t.plans, lang);
  fillPayments(t.payments);
  fillFaq(t.faq);

  const langPath = lang === "ru" ? "ru" : "en";
  document.querySelector("#link-terms").textContent = t.legalLinks[0];
  document.querySelector("#link-privacy").textContent = t.legalLinks[1];
  document.querySelector("#link-refund").textContent = t.legalLinks[2];
  document.querySelector("#link-contacts").textContent = t.legalLinks[3];
  document.querySelector("#link-help").textContent = t.legalLinks[4];
  document.querySelector("#link-terms").href = `${langPath}/terms.html`;
  document.querySelector("#link-privacy").href = `${langPath}/privacy.html`;
  document.querySelector("#link-refund").href = `${langPath}/refund.html`;
  document.querySelector("#link-contacts").href = `${langPath}/contacts.html`;
  document.querySelector("#link-help").href = `${langPath}/instructions.html`;

  document.querySelectorAll(".lang-btn").forEach((btn) => btn.classList.toggle("active", btn.dataset.lang === lang));
}

function wireConfig() {
  document.querySelector("#cta-open-bot").href = SITE_CONFIG.botUrl;
  document.querySelector("#header-open-bot").href = SITE_CONFIG.botUrl;
  document.querySelector("#cta-open-bot-bottom").href = SITE_CONFIG.botUrl;
  document.querySelector("#supportLink").href = SITE_CONFIG.supportUrl;
  document.querySelector("#supportEmail").href = `mailto:${SITE_CONFIG.supportEmail}`;
  document.querySelector("#supportEmail").textContent = SITE_CONFIG.supportEmail;
  document.querySelector("#year").textContent = String(new Date().getFullYear());
}

wireConfig();
setLanguage("ru");
document.querySelectorAll(".lang-btn").forEach((button) => {
  button.addEventListener("click", () => setLanguage(button.dataset.lang));
});
