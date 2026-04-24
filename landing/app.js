const SITE_CONFIG = {
  botUrl: "https://t.me/YOUR_BOT_USERNAME",
  supportUrl: "https://t.me/YOUR_SUPPORT_USERNAME",
  supportEmail: "support@example.com",
};

const CONTENT = {
  ru: {
    positioning: "Ваш AI-помощник в Telegram",
    heroTitle: "ViMi — ваш AI-помощник для Telegram",
    heroSubtitle: "Автоматизирует публикации, обрабатывает видео, показывает тарифы, создаёт счета и помогает управлять каналами в одном боте.",
    openBot: "🚀 Открыть бота",
    viewPlans: "💎 Смотреть тарифы",
    microTrust: "Надёжно · Работает в Telegram · Поддержка 24/7",
    navFeatures: "Возможности",
    navHow: "Как работает",
    navPlans: "Тарифы",
    navPayments: "Оплата",
    navFaq: "FAQ",
    floatAuto: "📣 Автопостинг",
    floatVideo: "🎬 Видео",
    floatAnalytics: "📊 Аналитика",
    floatMonet: "💰 Монетизация",
    featuresTitle: "Что умеет ViMi",
    features: [
      ["📣", "Автопостинг", "Публикации по расписанию.", "Контент выходит вовремя без рутины."],
      ["🎬", "Видео", "Обработка и подготовка видео.", "Готовый формат для Telegram."],
      ["💎", "Тарифы", "Гибкие планы для роста.", "Легко выбрать нужный объём."],
      ["💳", "Счета и оплата", "Счета внутри бота.", "Удобная оплата разными способами."],
      ["📊", "Статистика", "Понятная картина активности.", "Оценка динамики по каналам."],
      ["⚙️", "Автоматизация", "Меньше повторяющихся действий.", "Больше времени на контент."],
    ],
    howTitle: "Как работает",
    steps: [
      ["🤖", "Откройте бота", "Запустите ViMi в Telegram."],
      ["➕", "Добавьте источники и каналы", "Укажите, откуда брать и куда публиковать."],
      ["🧩", "Создайте правило публикации", "Настройте формат и расписание."],
      ["💳", "Подключите тариф или оплату", "Выберите подходящий вариант оплаты."],
      ["🚀", "ViMi выполняет рутину за вас", "Сервис берёт повторяющиеся задачи на себя."],
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
    paymentsStrip: ["Telegram Payments / Stars", "PayPal", "Visa", "MasterCard", "МИР", "СБП", "Crypto", "Lava.top", "Tribute"],
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
    paymentsDisclaimer: "Некоторые способы оплаты работают автоматически, некоторые — через ручное подтверждение.",
    faqTitle: "FAQ",
    faq: [
      ["Что такое ViMi?", "ViMi — Telegram-бот для автоматизации публикаций, видео и оплаты."],
      ["Это сайт или Telegram-бот?", "Сайт показывает возможности, основная работа происходит внутри Telegram-бота ViMi."],
      ["Можно ли использовать несколько каналов?", "Да, количество зависит от выбранного тарифа."],
      ["Что происходит при превышении лимитов?", "Бот покажет уведомление и предложит подходящий тариф."],
      ["Как работает оплата?", "Часть способов доступна автоматически, часть может подтверждаться вручную."],
      ["Можно ли вернуть деньги?", "Возврат рассматривается по правилам страницы Refund."],
      ["Как связаться с поддержкой?", "Через ссылку Telegram Support или по email внизу страницы."],
    ],
    finalCtaTitle: "Готовы автоматизировать ваши Telegram-каналы?",
    finalCtaSubtitle: "Откройте ViMi в Telegram и настройте первый сценарий за несколько минут.",
    footerDesc: "ViMi — premium landing v1 для удобной автоматизации Telegram-каналов.",
    footerProduct: "Продукт",
    footerLegal: "Документы",
    footerSupport: "Поддержка",
    legalLinks: ["Terms", "Privacy", "Refund", "Contacts", "Instructions"],
  },
  en: {
    positioning: "Your assistant in Telegram",
    heroTitle: "ViMi — your AI assistant for Telegram",
    heroSubtitle: "Automates publishing, processes videos, shows plans, creates invoices and helps manage channels in one bot.",
    openBot: "🚀 Open bot",
    viewPlans: "💎 View plans",
    microTrust: "Reliable · Works in Telegram · 24/7 support",
    navFeatures: "Features",
    navHow: "How it works",
    navPlans: "Plans",
    navPayments: "Payments",
    navFaq: "FAQ",
    floatAuto: "📣 Autopublishing",
    floatVideo: "🎬 Video",
    floatAnalytics: "📊 Analytics",
    floatMonet: "💰 Monetization",
    featuresTitle: "What ViMi can do",
    features: [
      ["📣", "Autopublishing", "Scheduled publishing to channels.", "Posts go live without manual routine."],
      ["🎬", "Video", "Video processing and prep.", "Content is ready for Telegram format."],
      ["💎", "Plans", "Flexible plans for growth.", "Choose the right scale for your channel."],
      ["💳", "Invoices and payments", "Invoices directly in the bot.", "Pay using available payment options."],
      ["📊", "Statistics", "Clear visibility of activity.", "Track your channel dynamics."],
      ["⚙️", "Automation", "Less repetitive manual actions.", "More time for creative work."],
    ],
    howTitle: "How it works",
    steps: [
      ["🤖", "Open the bot", "Launch ViMi in Telegram."],
      ["➕", "Add sources and channels", "Set where content comes from and where it goes."],
      ["🧩", "Create a publishing rule", "Choose schedule and delivery format."],
      ["💳", "Connect a plan or payment", "Pick a plan and payment method."],
      ["🚀", "ViMi handles routine work for you", "The service takes repetitive operations."],
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
    paymentsStrip: ["Telegram Payments / Stars", "PayPal", "Visa", "MasterCard", "MIR", "SBP", "Crypto", "Lava.top", "Tribute"],
    payments: [
      "Telegram Payments / Stars",
      "PayPal",
      "Bank cards",
      "SBP",
      "Cryptocurrency",
      "Tribute",
      "Lava.top",
      "Manual confirmation",
    ],
    paymentsDisclaimer: "Some payment methods work automatically, while others may require manual confirmation.",
    faqTitle: "FAQ",
    faq: [
      ["What is ViMi?", "ViMi is a Telegram bot for publishing, video and payment automation."],
      ["Is it a website or a Telegram bot?", "The website is a product page, while core actions happen inside the ViMi Telegram bot."],
      ["Can I use multiple channels?", "Yes, channel volume depends on your selected plan."],
      ["What happens when limits are exceeded?", "The bot notifies you and suggests an upgraded plan."],
      ["How does payment work?", "Some payment methods are automatic and some may require manual confirmation."],
      ["Can I request a refund?", "Refund requests are reviewed according to the Refund page terms."],
      ["How can I contact support?", "Use Telegram Support or email from the footer section."],
    ],
    finalCtaTitle: "Ready to automate your Telegram channels?",
    finalCtaSubtitle: "Open ViMi in Telegram and set up your first workflow in minutes.",
    footerDesc: "ViMi — premium landing v1 for Telegram channel automation.",
    footerProduct: "Product",
    footerLegal: "Legal",
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
    card.innerHTML = `<div class="feature-icon">${item[0]}</div><h3>${item[1]}</h3><p>${item[2]}<br>${item[3]}</p>`;
    grid.appendChild(card);
  });
}

function fillSteps(items) {
  const grid = document.querySelector("#stepsGrid");
  grid.innerHTML = "";
  items.forEach((item, idx) => {
    const card = document.createElement("article");
    card.className = "step-card";
    card.innerHTML = `<div class="step-number">${idx + 1}</div><h3>${item[0]} ${item[1]}</h3><p>${item[2]}</p>`;
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

function fillStripBadges(items) {
  const node = document.querySelector("#stripBadges");
  node.innerHTML = "";
  items.forEach((item) => {
    const badge = document.createElement("span");
    badge.className = "pay-badge";
    badge.textContent = item;
    node.appendChild(badge);
  });
}

function fillFaq(items) {
  const list = document.querySelector("#faqList");
  list.innerHTML = "";
  items.forEach((entry) => {
    const item = document.createElement("article");
    item.className = "faq-item";
    item.innerHTML = `<button class="faq-question" type="button">${entry[0]}</button><div class="faq-answer"><p>${entry[1]}</p></div>`;
    list.appendChild(item);
  });

  list.querySelectorAll(".faq-question").forEach((button) => {
    button.addEventListener("click", () => {
      const parent = button.closest(".faq-item");
      parent.classList.toggle("open");
    });
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
  fillStripBadges(t.paymentsStrip);
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

  document.querySelectorAll(".lang-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === lang);
  });
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
