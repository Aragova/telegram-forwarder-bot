/* ViMi Elements: tiny dependency-free progressive enhancements. */
(() => {
  "use strict";

  const dict = {
    en: {
      "common.skip": "Skip to content",
      "auto.heroTitle": "Autoposting without the busywork.",
      "auto.heroLead": "Posts, reposts and videos on your terms.",
      "auto.viewPricing": "View plans",
      "auto.flag1": "Scheduling",
      "auto.flag2": "Video",
      "auto.flag3": "Queues",
      "auto.sectionCapabilities": "Features",
      "auto.featuresTitle": "Everything for publishing.",
      "auto.featuresLead": "Six tools in one Telegram bot.",
      "auto.feature1Title": "Autoposting",
      "auto.feature1Text": "Scheduled posts from selected sources.",
      "auto.feature2Title": "Video",
      "auto.feature2Text": "Video, covers, captions and queues.",
      "auto.feature3Title": "Plans",
      "auto.feature3Text": "Plans and limits for channels and tasks.",
      "auto.feature4Title": "Invoices & payments",
      "auto.feature4Text": "Invoices and several payment options.",
      "auto.feature5Title": "Analytics",
      "auto.feature5Text": "Activity, usage and performance.",
      "auto.feature6Title": "Automation",
      "auto.feature6Text": "Less busywork, more time for content.",
      "auto.howLabel": "How it works",
      "auto.howTitle": "Get started in 5 steps.",
      "auto.howLead": "Open, configure and publish.",
      "auto.step1Title": "Open the bot",
      "auto.step1Text": "Start ViMi in Telegram.",
      "auto.step2Title": "Add channels",
      "auto.step2Text": "Choose sources and destinations.",
      "auto.step3Title": "Create a rule",
      "auto.step3Text": "Set the workflow and schedule.",
      "auto.step4Title": "Choose a plan",
      "auto.step4Text": "Pick the plan that fits.",
      "auto.step5Title": "Done",
      "auto.step5Text": "ViMi handles queues and publishing.",
      "auto.demoPreview": "Illustrative bot preview",
      "auto.demoRule": "Publishing rule",
      "auto.demoRuleDesc": "Source → Channel · Every 30 min",
      "auto.demoVideo": "Video processed",
      "auto.demoVideoDesc": "Cover and caption ready. Post queued.",
      "auto.demoStatus": "Status",
      "auto.demoRunning": "Active",
      "auto.demoQueue": "In queue",
      "auto.demoQueued": "12 posts",
      "auto.plansLabel": "Autoposting plans",
      "auto.plansTitle": "Choose your plan.",
      "auto.plansLead": "FREE, BASIC and PRO. These plans apply only to ViMi Autoposting.",
      "auto.plan1Desc": "Get started",
      "auto.month": "month",
      "auto.plan1Item1": "3 rules",
      "auto.plan1Item2": "5 videos / day",
      "auto.plan1Item3": "100 tasks / day",
      "auto.choose": "Choose in bot",
      "auto.popular": "Popular",
      "auto.plan2Desc": "For regular work",
      "auto.plan2Item1": "15 rules",
      "auto.plan2Item2": "30 videos / day",
      "auto.plan2Item3": "1000 tasks / day",
      "auto.plan3Desc": "For larger channels",
      "auto.plan3Item1": "50 rules",
      "auto.plan3Item2": "100 videos / day",
      "auto.plan3Item3": "5000 tasks / day",
      "auto.pricingFootnote": "Prices and limits are preserved from the previous site. Check the current terms in the bot before paying.",
      "auto.paymentTitle": "Payment methods",
      "auto.method1": "Telegram Stars",
      "auto.method2": "PayPal",
      "auto.method3": "Bank cards",
      "auto.method4": "SBP",
      "auto.method5": "Cryptocurrency",
      "auto.method6": "Tribute",
      "auto.method7": "Lava.top",
      "auto.method8": "Manual payment",
      "auto.paymentNote": "Check available methods in the bot. Some payments require manual confirmation.",
      "auto.ctaTitle": "Publish without busywork.",
      "auto.ctaLead": "ViMi runs in Telegram.",
      "common.brandTagline": "ELEMENTAL ECOSYSTEM",
      "common.home": "Home",
      "common.autoposting": "ViMi Autoposting",
      "common.social": "ViMi Social",
      "common.pay": "ViMi Pay",
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
      "home.tag": "ViMi · Three elements",
      "home.flowLabel": "FLOW",
      "home.energyLabel": "ENERGY",
      "home.valueLabel": "VALUE",
      "home.waterLabel": "01 · WATER",
      "home.fireLabel": "02 · FIRE",
      "home.earthLabel": "03 · EARTH",
      "home.waterSkyLabel": "WATER / SKY",
      "home.fireEnergyLabel": "FIRE / ENERGY",
      "home.earthValueLabel": "EARTH / VALUE",
      "home.title1": "ViMi.",
      "home.title2": "Three products.",
      "home.lead": "Autoposting, social media and monetization in Telegram.",
      "home.explore": "Choose a product",
      "home.start": "ViMi Autoposting",
      "home.sectionLabel": "ViMi ecosystem",
      "home.sectionTitle": "Your tool.",
      "home.sectionLead": "Three products. Make your choice.",
      "home.autopostingSummary": "Posts, reposts and video without busywork.",
      "home.socialSummary": "AI discovery and content for social media.",
      "home.paySummary": "Memberships, channels and file sales.",
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
      "home.ctaTitle": "Your element. Your ViMi.",
      "home.ctaText": "Start with the product that solves your challenge today.",
      "home.ctaButton": "Explore products",

      "autoposting.tag": "01 / Water & sky",
      "autoposting.elementLabel": "WATER · ViMi",
      "autoposting.ctaElement": "FLOW",
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
      "autoposting.pricingLabel": "Autoposting pricing",
      "autoposting.pricingTitle": "Find your rhythm.",
      "autoposting.pricingLead": "Check the ViMi Autoposting bot for current plans, limits and payment methods. These terms belong to Autoposting, not ViMi Social or ViMi Pay.",
      "autoposting.pricingButton": "See Telegram pricing",

      "social.tag": "02 / Fire",
      "social.elementLabel": "FIRE · ViMi",
      "social.ctaElement": "ENERGY",
      "social.headline1": "Content,",
      "social.headline2": "faster.",
      "social.lead": "AI discovery and content preparation for social platforms.",
      "social.button": "Contact support",
      "social.extra": "Explore the ecosystem",
      "social.note": "ViMi Social is being tested. Ask support about access.",
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
      "pay.elementLabel": "EARTH · ViMi",
      "pay.ctaElement": "VALUE",
      "pay.headline1": "Monetization",
      "pay.headline2": "in Telegram.",
      "pay.lead": "Memberships, private channels and digital file sales.",
      "pay.button": "Open ViMi Pay",
      "pay.extra": "Explore capabilities",
      "pay.note": "ViMi Pay is in testing. Onboarding may be limited.",
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
      "pay.ctaText": "Manage everything from the central Telegram bot.",
      "footer.instructions": "Autoposting instructions",
      "autoposting.faqLabel": "Frequently asked questions",
      "autoposting.faqTitle": "The essentials, explained.",
      "autoposting.faqLead": "Clear answers without marketing promises.",
      "autoposting.q1": "How do I get started?",
      "autoposting.a1": "Open the bot, connect a source and channel, then set a publishing rule and interval.",
      "autoposting.q2": "Where can I see the pricing?",
      "autoposting.a2": "Current terms and pricing are available in Autoposting. They do not apply to Social or Pay.",
      "autoposting.q3": "Can it handle video?",
      "autoposting.a3": "Yes. Video workflows and publishing queues are supported. Availability depends on your current plan.",
      "social.faqLabel": "Frequently asked questions",
      "social.faqTitle": "The essentials, explained.",
      "social.faqLead": "Clear answers without marketing promises.",
      "social.q1": "Can I join now?",
      "social.a1": "ViMi Social is in testing. Ask support about access.",
      "social.q2": "What does AI Search do?",
      "social.a2": "It helps discover materials for a topic and task from available, authorized sources.",
      "social.q3": "Does it publish automatically?",
      "social.a3": "The currently implemented flow includes preview and approval before publishing.",
      "pay.faqLabel": "Frequently asked questions",
      "pay.faqTitle": "The essentials, explained.",
      "pay.faqLead": "Clear answers without marketing promises.",
      "pay.q1": "What can I sell?",
      "pay.a1": "ViMi Pay provides tools for private-channel memberships and digital files, including free distribution.",
      "pay.q2": "How are payments approved?",
      "pay.a2": "Owners configure available payment methods. Administrators confirm manual payments.",
      "pay.q3": "Is public registration open?",
      "pay.a3": "The platform is still being tested. New onboarding may be limited."
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

  const elements = [...document.querySelectorAll("[data-i18n]")];
  const original = new Map(elements.map((el) => [el.dataset.i18n, el.textContent]));
  // Preserve the original landing contract: dict.ru / dict.en + data-i18n.
  dict.ru = Object.fromEntries(original);
  const page = document.body.dataset.page || "home";

  function applyLanguage(lang) {
    const current = lang === "en" ? "en" : "ru";
    document.documentElement.lang = current;
    // Restore the original ViMi robot assets, using their RU/EN variants.
    for (const robot of document.querySelectorAll("[data-robot]")) {
      const src = robot.getAttribute("src");
      if (!src || !/hero-(robot|scene)-(ru|en)\.png$/.test(src)) continue;
      const next = src.replace(/-(ru|en)\.png$/, "-" + current + ".png");
      if (src !== next) robot.setAttribute("src", next);
      if (robot.getAttribute("alt")) robot.setAttribute("alt", current === "en" ?
        "ViMi, the robot mascot" : "ViMi, робот-персонаж");
    }

    for (const el of elements) {
      const key = el.dataset.i18n;
      const translated = dict[current][key];
      if (typeof translated === "string") el.textContent = translated;
    }
    for (const button of document.querySelectorAll("[data-lang-choice]")) {
      button.setAttribute("aria-pressed", String(button.dataset.langChoice === current));
    }
    const navigation = document.getElementById("main-nav");
    if (navigation) navigation.setAttribute("aria-label", current === "en" ? "ViMi products" : "Продукты ViMi");
    const homeBrand = document.querySelector(".site-header .brand");
    if (homeBrand) homeBrand.setAttribute("aria-label", current === "en" ? "ViMi, home" : "ViMi, главная");
    for (const link of document.querySelectorAll("[data-doc]")) {
      const before = link.getAttribute("href");
      link.setAttribute("href", before.replace(/(?:ru|en)\/(terms|privacy|refund|contacts|instructions)\.html$/, current + "/$1.html"));
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
    try { localStorage.setItem("vimi_lang", current); } catch (_) { /* Storage can be blocked. */ }
  }

  for (const button of document.querySelectorAll("[data-lang-choice]")) {
    button.addEventListener("click", () => applyLanguage(button.dataset.langChoice));
  }
  let saved = "ru";
  try { saved = localStorage.getItem("vimi_lang") || "ru"; } catch (_) { /* Private mode. */ }
  applyLanguage(saved);

  // All three products remain visible as a horizontally scrollable row on mobile.
  // Keep the active product centered within that row without moving the document.
  const productNav = document.getElementById("main-nav");
  const selectedProduct = productNav?.querySelector('[aria-current="page"]');
  if (productNav && selectedProduct && productNav.scrollWidth > productNav.clientWidth) {
    productNav.scrollLeft = Math.max(0,
      selectedProduct.offsetLeft - productNav.offsetLeft -
      (productNav.clientWidth - selectedProduct.clientWidth) / 2);
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
