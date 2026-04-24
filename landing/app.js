const SITE_CONFIG = {
  botUrl: "https://t.me/YOUR_BOT_USERNAME",
  supportUrl: "https://t.me/YOUR_SUPPORT_USERNAME",
  supportEmail: "support@example.com",
  legalOwner: "YOUR LEGAL ENTITY / NAME",
};

const CONTENT = {
  ru: {
    heroTitle: "ChannelPilot — автоматизация Telegram-каналов в одном боте",
    heroSubtitle: "Публикации, видео, очереди, тарифы, счета и оплата — всё внутри Telegram.",
    openBot: "Открыть бота",
    viewPlans: "Посмотреть тарифы",
    modulesTitle: "Product modules",
    modules: [
      ["📣 Автопостинг", "Автоматическая публикация из источников в каналы и группы."],
      ["🎬 Видео", "Обработка видео, заставки, подписи и очередь."],
      ["💎 SaaS-тарифы", "Лимиты, usage, счета и подписки внутри бота."],
      ["💳 Оплата", "Telegram, PayPal, карта, СБП, криптовалюта и ручное подтверждение."],
    ],
    audienceTitle: "Кому полезно",
    audience: [
      "владельцам Telegram-каналов",
      "администраторам сеток каналов",
      "SMM-командам",
      "видео-проектам",
      "платным каналам",
      "авторам и экспертам",
    ],
    flowTitle: "Как работает",
    flow: [
      "Откройте бота",
      "Добавьте источник и получателя",
      "Создайте правило публикации",
      "Подключите тариф или оплату",
      "Бот выполняет рутину автоматически",
    ],
    featuresTitle: "Возможности",
    features: ["Очередь публикаций", "Расписание", "Видео pipeline", "Подписи", "Тарифы и лимиты", "Счета и оплата", "Живой статус", "RU/EN интерфейс"],
    plansTitle: "Тарифы",
    plans: [
      { name: "FREE", desc: "для теста", limits: ["3 правила", "5 видео/день", "100 задач/день"], price: "0 USD" },
      { name: "BASIC", desc: "для стабильной автопубликации", limits: ["15 правил", "30 видео/день", "1000 задач/день"], price: "9 USD / месяц" },
      { name: "PRO", desc: "для больших каналов и видео", limits: ["50 правил", "100 видео/день", "5000 задач/день"], price: "29 USD / месяц" },
    ],
    choosePlan: "Выбрать в боте",
    paymentsTitle: "Способы оплаты",
    payments: ["Telegram Payments / Stars", "PayPal", "Банковская карта", "СБП", "Криптовалюта", "Tribute", "Lava.top", "Ручное подтверждение"],
    paymentsDisclaimer: "Некоторые способы оплаты могут работать автоматически, а некоторые — через ручное подтверждение администратором.",
    instructionsTitle: "Инструкции",
    instructionsPage: "Открыть полные инструкции",
    instructions: ["Как открыть бота", "Как создать первое правило", "Как посмотреть тариф", "Как создать счёт", "Как оплатить", "Как обратиться в поддержку"],
    faqTitle: "FAQ",
    faq: [
      ["Это отдельный сайт или Telegram-бот?", "Это сайт-витрина. Основная работа происходит внутри Telegram-бота."],
      ["Нужно ли давать доступ к каналам?", "Да, для автоматизации нужно настроить права бота там, где будет публикация."],
      ["Можно ли использовать несколько каналов?", "Да, можно добавлять несколько источников и получателей по вашему тарифу."],
      ["Что происходит при превышении лимитов?", "Вы увидите ограничения в боте и сможете перейти на более высокий тариф."],
      ["Как работает оплата?", "Счёт создаётся в боте, затем вы оплачиваете удобным способом."],
      ["Какие способы оплаты доступны?", "Доступны Telegram, PayPal, карта, СБП, криптовалюта и партнёрские методы."],
      ["Можно ли вернуть деньги?", "Возврат рассматривается по правилам Refund Policy."],
      ["Как связаться с поддержкой?", "Через Telegram support или email из раздела Contacts."],
    ],
    trustTitle: "Trust / security",
    trust: [
      "Мы не храним данные банковских карт.",
      "Платёжные данные обрабатываются платёжными провайдерами.",
      "Для ручных оплат требуется подтверждение администратора.",
      "Тарифы, лимиты и счета видны в боте.",
      "Перед публикацией legal-тексты нужно проверить с юристом.",
    ],
    finalCtaTitle: "Готовы автоматизировать Telegram-каналы?",
    ownerLabel: "Юридический владелец",
    legalLinks: ["Условия", "Конфиденциальность", "Возврат", "Контакты", "Инструкции"],
  },
  en: {
    heroTitle: "ChannelPilot — Telegram channel automation in one bot",
    heroSubtitle: "Publishing, video processing, queues, plans, invoices and payments — all inside Telegram.",
    openBot: "Open bot",
    viewPlans: "View plans",
    modulesTitle: "Product modules",
    modules: [["📣 Autopublishing", "Automatic posting from sources to channels and groups."], ["🎬 Video", "Video processing, covers, captions and queue."], ["💎 SaaS plans", "Limits, usage, invoices and subscriptions inside the bot."], ["💳 Payments", "Telegram, PayPal, card, SBP, crypto and manual confirmation."]],
    audienceTitle: "Who it is for",
    audience: ["Telegram channel owners", "channel network admins", "SMM teams", "video projects", "paid channels", "creators and experts"],
    flowTitle: "How it works",
    flow: ["Open the bot", "Add a source and destination", "Create a publishing rule", "Choose a plan or payment", "The bot handles routine automatically"],
    featuresTitle: "Features",
    features: ["Publication queue", "Scheduling", "Video pipeline", "Captions", "Plans and limits", "Invoices and payments", "Live status", "RU/EN interface"],
    plansTitle: "Plans",
    plans: [
      { name: "FREE", desc: "for testing", limits: ["3 rules", "5 videos/day", "100 tasks/day"], price: "0 USD" },
      { name: "BASIC", desc: "for stable autopublishing", limits: ["15 rules", "30 videos/day", "1000 tasks/day"], price: "9 USD / month" },
      { name: "PRO", desc: "for large channels and video", limits: ["50 rules", "100 videos/day", "5000 tasks/day"], price: "29 USD / month" },
    ],
    choosePlan: "Choose in bot",
    paymentsTitle: "Payment methods",
    payments: ["Telegram Payments / Stars", "PayPal", "Bank card", "SBP", "Cryptocurrency", "Tribute", "Lava.top", "Manual confirmation"],
    paymentsDisclaimer: "Some payment methods may work automatically, while others may require manual administrator confirmation.",
    instructionsTitle: "Instructions",
    instructionsPage: "Open full instructions",
    instructions: ["How to open the bot", "How to create the first rule", "How to view your plan", "How to create an invoice", "How to pay", "How to contact support"],
    faqTitle: "FAQ",
    faq: [
      ["Is this a website or a Telegram bot?", "This is a showcase website. Main product actions are done inside the Telegram bot."],
      ["Do I need to grant channel access?", "Yes, bot permissions are required for channels where publishing is configured."],
      ["Can I use multiple channels?", "Yes, you can connect multiple sources and destinations based on your plan."],
      ["What happens when limits are exceeded?", "You will see limit messages in the bot and can upgrade your plan."],
      ["How does payment work?", "You create an invoice in the bot and pay using a supported method."],
      ["Which payment methods are available?", "Telegram, PayPal, card, SBP, crypto and partner methods are available."],
      ["Can I request a refund?", "Refund requests are reviewed under the Refund Policy."],
      ["How can I contact support?", "Use Telegram support or email from the Contacts page."],
    ],
    trustTitle: "Trust / security",
    trust: [
      "We do not store bank card data.",
      "Payment data is processed by payment providers.",
      "Manual payments require administrator confirmation.",
      "Plans, limits and invoices are visible in the bot.",
      "Legal texts must be reviewed with a lawyer before publication.",
    ],
    finalCtaTitle: "Ready to automate your Telegram channels?",
    ownerLabel: "Legal owner",
    legalLinks: ["Terms", "Privacy", "Refund", "Contacts", "Instructions"],
  }
};

function fillList(selector, items, asCards = false) {
  const container = document.querySelector(selector);
  container.innerHTML = "";
  items.forEach((item) => {
    const el = document.createElement(asCards ? "div" : "li");
    if (asCards) {
      el.className = "card";
      if (Array.isArray(item)) {
        el.innerHTML = `<strong>${item[0]}</strong><p>${item[1]}</p>`;
      } else {
        el.textContent = item;
      }
    } else {
      el.textContent = item;
    }
    container.appendChild(el);
  });
}

function fillFaq(items) {
  const node = document.querySelector("#faqList");
  node.innerHTML = "";
  items.forEach((entry) => {
    const item = document.createElement("div");
    item.className = "faq-item";
    item.innerHTML = `<strong>${entry[0]}</strong><span>${entry[1]}</span>`;
    node.appendChild(item);
  });
}

function fillPlans(data, lang) {
  const grid = document.querySelector("#plansGrid");
  grid.innerHTML = "";
  data.forEach((plan) => {
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = `<strong>${plan.name}</strong><p>${plan.desc}</p><div class="price">${plan.price}</div><ul>${plan.limits.map((x) => `<li>${x}</li>`).join("")}</ul><a class="btn btn-primary" href="${SITE_CONFIG.botUrl}" target="_blank" rel="noopener">${CONTENT[lang].choosePlan}</a>`;
    grid.appendChild(card);
  });
}

function setLanguage(lang) {
  const t = CONTENT[lang];
  document.documentElement.lang = lang;
  document.querySelectorAll("[data-i18n]").forEach((node) => {
    node.textContent = t[node.dataset.i18n];
  });

  fillList("#modulesGrid", t.modules, true);
  fillList("#audienceList", t.audience);
  fillList("#flowList", t.flow);
  fillList("#featuresGrid", t.features, true);
  fillPlans(t.plans, lang);
  fillList("#paymentsList", t.payments);
  fillList("#instructionsList", t.instructions);
  fillFaq(t.faq);
  fillList("#trustList", t.trust);

  const langPath = lang === "ru" ? "ru" : "en";
  document.querySelector("#instructionsLink").href = `${langPath}/instructions.html`;
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
  document.querySelector("#cta-open-bot-bottom").href = SITE_CONFIG.botUrl;
  document.querySelector("#supportLink").href = SITE_CONFIG.supportUrl;
  document.querySelector("#supportEmail").href = `mailto:${SITE_CONFIG.supportEmail}`;
  document.querySelector("#supportEmail").textContent = SITE_CONFIG.supportEmail;
  document.querySelector("#legalOwner").textContent = SITE_CONFIG.legalOwner;
  document.querySelector("#year").textContent = String(new Date().getFullYear());
}

wireConfig();
setLanguage("ru");
document.querySelectorAll(".lang-btn").forEach((button) => {
  button.addEventListener("click", () => setLanguage(button.dataset.lang));
});
