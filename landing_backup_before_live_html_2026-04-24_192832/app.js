const SITE_CONFIG = {
  botUrl: "https://t.me/topposter69_bot",
  supportUrl: "https://t.me/YOUR_SUPPORT_USERNAME",
  supportEmail: "support@example.com",
  legalOwner: "YOUR LEGAL ENTITY / NAME"
};

document.querySelectorAll('[data-link="bot"]').forEach((link) => {
  link.href = SITE_CONFIG.botUrl;
});
