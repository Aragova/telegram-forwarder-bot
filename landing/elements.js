/* ViMi Elements: tiny dependency-free progressive enhancements. */
(() => {
  "use strict";

  const translations = {
    en: {
      "common.skip": "Skip to content",
      "common.home": "Home",
      "common.autoposting": "Autoposting",
      "common.social": "Social",
      "common.pay": "Pay",
      "common.learn": "Explore product",
      "footer.summary": "Three independent products for content, audiences and monetization. One ecosystem.",
      "footer.products": "Products",
      "footer.support": "Support ↗",
      "footer.documents": "Documents & contacts",
      "footer.terms": "Autoposting terms",
      "footer.privacy": "Privacy",
      "footer.refund": "Refunds",
      "footer.contact": "Contact",
      "footer.note": "Products have separate terms and pricing.",
      "home.tag": "Three elements. One ecosystem.",
      "home.title1": "Own your",
      "home.title2": "element.",
      "home.lead": "Create content, automate publishing and turn your audience into value. Three independent tools united by the ViMi philosophy.",
      "home.explore": "Explore the products",
      "home.start": "Start with Autoposting",
      "home.sectionLabel": "ViMi ecosystem",
      "home.sectionTitle": "Three products. Three personalities.",
      "home.sectionLead": "Every element has its own purpose. Choose the right tool without mixing product settings and pricing.",
      "home.autopostingSummary": "Publishing, reposts and video on your terms. Let your content flow without routine work.",
      "home.socialSummary": "AI-powered discovery, preparation and adaptation of content for social platforms.",
      "home.paySummary": "Private channels, subscriptions and digital file sales through Telegram.",
      "home.benefit1": "One distinctive brand",
      "home.benefitText1": "A consistent visual language and simple navigation between products.",
      "home.benefit2": "Independent capabilities",
      "home.benefitText2": "Dedicated tools, clear pricing and separate workflows.",
      "home.benefit3": "Telegram at the center",
      "home.benefitText3": "Every product is designed around the familiar bot experience.",
      "home.visionLabel": "The ViMi philosophy",
      "home.visionTitle": "Technology working for your ideas.",
      "home.visionLead": "Not just features, but three ways to grow: flow, energy and value.",
      "home.flowTitle": "An uninterrupted flow",
      "home.flowText": "Autoposting takes repetitive publishing work off your plate.",
      "home.fireTitle": "Creative energy",
      "home.fireText": "Social helps discover and prepare material for selected platforms.",
      "home.valueTitle": "Audience value",
      "home.valueText": "Pay gives creators tools for memberships and digital files.",
      "home.ctaLabel": "ViMi Elements",
      "home.ctaTitle": "Choose your element. Let technology do the rest.",
      "home.ctaText": "Start with the product that solves your challenge today.",
      "home.ctaButton": "Choose a product",

      "autoposting.tag": "01 / Water & sky",
      "autoposting.headline1": "Let your content",
      "autoposting.headline2": "move on its own.",
      "autoposting.lead": "Automated posts, queues and publishing controls. Build a content flow that follows your rules.",
      "autoposting.button": "Open Telegram bot",
      "autoposting.extra": "Full feature overview",
      "autoposting.note": "An existing product. Autoposting pricing does not apply to Social or Pay.",
      "autoposting.flag1": "Automated posts",
      "autoposting.flag2": "Scheduling",
      "autoposting.flag3": "Publishing queues",
      "autoposting.detailsLabel": "Autoposting capabilities",
      "autoposting.detailsTitle": "Keep the creative energy. Automate the routine.",
      "autoposting.detailsLead": "Core tools for managing Telegram channels.",
      "autoposting.f1Title": "Publishing rules",
      "autoposting.f1Text": "Configure sources, destinations and the publishing order.",
      "autoposting.f2Title": "Schedule & queue",
      "autoposting.f2Text": "Manage delivery intervals and queues independently for each rule.",
      "autoposting.f3Title": "Video & reposts",
      "autoposting.f3Text": "Prepare content and work with videos and reposts in a familiar interface.",
      "autoposting.ctaTitle": "Let the content flow.",
      "autoposting.ctaText": "ViMi Autoposting is accessible through Telegram.",

      "social.tag": "02 / Fire",
      "social.headline1": "Spark",
      "social.headline2": "their attention.",
      "social.lead": "AI-powered discovery, preparation and adaptation for social platforms. Give your ideas motion and form.",
      "social.button": "Contact support",
      "social.extra": "Explore the ecosystem",
      "social.note": "ViMi Social is in testing. Public launch and pricing have not been announced.",
      "social.flag1": "AI Search",
      "social.flag2": "Media Preparation",
      "social.flag3": "Human Approval",
      "social.detailsLabel": "Social capabilities",
      "social.detailsTitle": "From idea to ready-to-use material.",
      "social.detailsLead": "The product direction reflected in its current roadmap.",
      "social.f1Title": "Ideas & source discovery",
      "social.f1Text": "AI Search helps find materials for goals and selected sources.",
      "social.f2Title": "Analysis & adaptation",
      "social.f2Text": "Prepare media and platform-specific variants.",
      "social.f3Title": "Publication control",
      "social.f3Text": "Preview and approve content before publishing to supported destinations.",
      "social.ctaTitle": "Be among the first.",
      "social.ctaText": "Follow the project's progress and ask support about availability.",

      "pay.tag": "03 / Earth",
      "pay.headline1": "Turn audiences",
      "pay.headline2": "into value.",
      "pay.lead": "Memberships, paid channels and digital files. Monetize Telegram with control over access and payments.",
      "pay.button": "Open ViMi Pay",
      "pay.extra": "Explore capabilities",
      "pay.note": "ViMi Pay is being tested. New customer onboarding may be limited.",
      "pay.flag1": "Memberships",
      "pay.flag2": "Digital files",
      "pay.flag3": "Access control",
      "pay.detailsLabel": "ViMi Pay capabilities",
      "pay.detailsTitle": "Your audience. Your access rules.",
      "pay.detailsLead": "Tools for independent digital creators.",
      "pay.f1Title": "Paid channels",
      "pay.f1Text": "Create plans for your Telegram channels and manage memberships.",
      "pay.f2Title": "File sales",
      "pay.f2Text": "Publish digital materials with paid or free access.",
      "pay.f3Title": "Payments & control",
      "pay.f3Text": "Configure available payment methods and approve manual payments.",
      "pay.ctaTitle": "Build value with ViMi Pay.",
      "pay.ctaText": "Manage everything from the central Telegram bot."
    }
  };

  const meta = {
    home: {
      ru: ["ViMi — три продукта, одна экосистема", "ViMi Elements: Autoposting, Social и Pay. Автоматизация публикаций, подготовка контента для социальных сетей и монетизация в Telegram."],
      en: ["ViMi — three products, one ecosystem", "ViMi Elements brings together Autoposting, Social and Pay. Automate publishing, prepare social content and monetize Telegram."]
    },
    autoposting: {
      ru: ["ViMi Autoposting — автоматизация публикаций Telegram", "Автопостинг, правила источников, очереди публикаций и работа с видео в Telegram. ViMi Autoposting."],
      en: ["ViMi Autoposting — automated Telegram publishing", "Automated publishing, source rules, post queues and video workflows for Telegram channels."]
    },
    social: {
      ru: ["ViMi Social — AI-поиск и подготовка контента", "ViMi Social: AI-поиск, подготовка медиа, адаптация под социальные платформы и контроль публикаций. Продукт проходит тестирование."],
      en: ["ViMi Social — AI discovery and content preparation", "ViMi Social: AI search, media preparation, social platform adaptation and review. The product is in testing."]
    },
    pay: {
      ru: ["ViMi Pay — подписки и продажа файлов в Telegram", "ViMi Pay: собственные Telegram-боты, платные каналы, подписки, продажа файлов и управление доступом. Платформа проходит тестирование."],
      en: ["ViMi Pay — Telegram memberships and digital file sales", "ViMi Pay: independent Telegram bots, paid channels, memberships, digital files and access controls. The platform is in testing."]
    }
  };

  const elements = [...document.querySelectorAll("[data-t]")];
  const original = new Map(elements.map((el) => [el.dataset.t, el.textContent]));
  const page = document.body.dataset.page || "home";

  function applyLanguage(lang) {
    const current = lang === "en" ? "en" : "ru";
    document.documentElement.lang = current;
    for (const el of elements) {
      const key = el.dataset.t;
      const translated = current === "en" ? translations.en[key] : original.get(key);
      if (typeof translated === "string") el.textContent = translated;
    }
    for (const button of document.querySelectorAll("[data-lang-choice]")) {
      button.setAttribute("aria-pressed", String(button.dataset.langChoice === current));
    }
    for (const link of document.querySelectorAll("[data-doc]")) {
      const before = link.getAttribute("href");
      link.setAttribute("href", before.replace(/(?:ru|en)\/(terms|privacy|refund|contacts)\.html$/, current + "/$1.html"));
    }
    const [title, description] = (meta[page] || meta.home)[current];
    document.title = title;
    for (const [selector, text] of [
      ['meta[name="description"]', description],
      ['meta[property="og:title"]', title],
      ['meta[property="og:description"]', description],
      ['meta[property="og:locale"]', current === "en" ? "en_US" : "ru_RU"]
    ]) {
      const node = document.querySelector(selector);
      if (node) node.setAttribute("content", text);
    }
    try { localStorage.setItem("vimi-elements-language", current); } catch (_) { /* Storage can be blocked. */ }
  }

  for (const button of document.querySelectorAll("[data-lang-choice]")) {
    button.addEventListener("click", () => applyLanguage(button.dataset.langChoice));
  }
  let saved = "ru";
  try { saved = localStorage.getItem("vimi-elements-language") || "ru"; } catch (_) { /* Private mode. */ }
  applyLanguage(saved);

  const toggle = document.querySelector(".menu-toggle");
  const nav = document.getElementById("main-nav");
  if (toggle && nav) {
    const close = () => {
      nav.dataset.open = "false";
      toggle.setAttribute("aria-expanded", "false");
      toggle.setAttribute("aria-label", document.documentElement.lang === "en" ? "Open menu" : "Открыть меню");
    };
    toggle.addEventListener("click", () => {
      const open = nav.dataset.open !== "true";
      nav.dataset.open = String(open);
      toggle.setAttribute("aria-expanded", String(open));
      toggle.setAttribute("aria-label", open
        ? (document.documentElement.lang === "en" ? "Close menu" : "Закрыть меню")
        : (document.documentElement.lang === "en" ? "Open menu" : "Открыть меню"));
    });
    nav.querySelectorAll("a").forEach(link => link.addEventListener("click", close));
    document.addEventListener("keydown", event => { if (event.key === "Escape") close(); });
    window.addEventListener("resize", () => { if (window.innerWidth > 780) close(); });
  }

  const reduced = window.matchMedia("(prefers-reduced-motion: reduce)");
  if (window.matchMedia("(hover: hover) and (pointer: fine)").matches && !reduced.matches) {
    for (const card of document.querySelectorAll("[data-tilt]")) {
      let scheduled = false;
      let position = null;
      card.addEventListener("pointermove", (event) => {
        const box = card.getBoundingClientRect();
        position = [(event.clientX - box.left) / box.width, (event.clientY - box.top) / box.height];
        if (scheduled) return;
        scheduled = true;
        requestAnimationFrame(() => {
          scheduled = false;
          if (!position) return;
          const [x, y] = position;
          card.style.setProperty("--ry", ((x - .5) * 7).toFixed(2) + "deg");
          card.style.setProperty("--rx", ((.5 - y) * 7).toFixed(2) + "deg");
          card.style.setProperty("--mx", (x * 100).toFixed(1) + "%");
          card.style.setProperty("--my", (y * 100).toFixed(1) + "%");
        });
      }, { passive: true });
      card.addEventListener("pointerleave", () => {
        position = null;
        card.style.removeProperty("--rx");
        card.style.removeProperty("--ry");
        card.style.removeProperty("--mx");
        card.style.removeProperty("--my");
      });
    }
  }
})();
