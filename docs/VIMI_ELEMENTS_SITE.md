# ViMi Elements — multi-product website (2026-11-16)

## Scope

The public website `usevimi.ru` is the shared **marketing storefront** for
three independent Telegram products. This PR changes **only the static landing
directory** and adds optional site-only QA. No bot services, databases,
external DNS, payment settings or nginx are modified.

| Page | Product | Element | Public status |
| --- | --- | --- | --- |
| `/` | ViMi ecosystem | All three | Marketing homepage |
| `/autoposting/` | ViMi Autoposting | Water & sky | Existing product |
| `/social/` | ViMi Social | Fire | Testing, no public bot URL published |
| `/pay/` | ViMi Pay | Earth & emerald | Testing, onboarding may be limited |

**App addresses are NOT website sections and must not be linked as public
landing pages.** Existing `api.usevimi.ru`, `staging.usevimi.ru`,
`pay-api.usevimi.ru` and their DNS records are unchanged.

## Architecture

- The root and each product page are pre-rendered, searchable, semantic HTML
  under `landing/`; no SPA/router/framework, build step or JS dependency.
- Shared `elements.css` / `elements.js` provide a premium dark identity,
  CSS-based water/fire/earth 3D sculptures and subtle pointer tilt.
- RU copy is present in the source HTML; EN translations are complete in
  `elements.js`, using the existing landing `data-i18n`/`dict` contract.
  The same legacy `vimi_lang` preference is used across all pages. A visitor
  switches languages without leaving the product page.
- Lightweight CSS transforms and gradients replace WebGL. Pointer effects
  are disabled for touch/non-hover devices, and all animation is disabled by
  `prefers-reduced-motion`. All links/buttons are keyboard focusable.
- Dedicated canonical URLs and existing social preview image are retained.
  Sitemap includes the three product pages.
- Existing SEO pages `/autoposting.html`, `/telegram-bot.html` and
  `/video-repost.html` retain their content; only their outdated root
  `#features/#how/#pricing` links now point to real Autoposting destinations.
  Existing `/ru/` and `/en/` legal/help pages, media assets, `CNAME`,
  old `app.js` and `styles.css` are not modified.
- Existing Telegram destinations: Autoposting `@topposter69_bot`, Pay
  `@vimipay_bot`; Social **does not invent a public bot** and instead links
  to existing `@vimi_support_bot`. Status notices avoid fictional product
  launch dates, pricing or payment guarantees.

## 2026-11-16 Visual and content restoration (PR #492)

After the first site release, its appearance and Autoposting information were
reviewed against the actual pre-#491 homepage
(`29d5a5634ddd1a52737e0304357137e9a7680731`).

- **ViMi is the original robot**: the existing
  `hero-robot-ru/en.png` and `hero-scene-ru/en.png` assets are visible in
  hero sections. No new surrogate robot or stock character was introduced.
  The existing language switch also changes the image locale.
- Header has two levels: ecosystem **Home** above **ViMi Autoposting**,
  **ViMi Social**, **ViMi Pay**. Narrow phones horizontally scroll only
  the product row, while the brand/Home/RU/EN remain accessible.
- Typography: Manrope for headings/buttons and Inter for readable body text.
  Product pages use visibly distinct blue water/sky, aubergine/coral fire,
  and emerald earth backgrounds. Spacing and hero copy are compact.
- Restored the **full Autoposting scope** from the old homepage:
  6 features (Autoposting, Video, Plans, Payments, Statistics, Automation),
  5 onboarding steps, illustrative bot preview, FREE/BASIC/PRO cards and their
  previously published 0/9/29 USD prices and corresponding limits, 8 payment
  method labels, FAQ and Telegram CTAs. These marketing prices are a
  **historical baseline**, not a live billing API; the UI reminds visitors to
  verify availability and price inside the bot.
- Old SEO page Instructions links and the new site's footer now target the
  actual `/autoposting/#how` guide instead of the existing placeholder help
  page. Other legal documents were not changed.
- Regression tests now guard the exact restored feature/step/plan/payment
  counts, price values, existing robot assets, RU/EN strings, product
  atmospheres, mobile navigation and internal links.

## Testing and release

Run from repository root:

```bash
node --check landing/elements.js
python3 -m unittest -v tests/test_elements_site.py
```

PR automatically runs `ViMi Elements | Website Quality` and existing
repository smoke checks. Perform manual desktop/mobile review of every page
at narrow and wide widths, RU/EN switch, focus navigation, status notes and
reduced-motion behavior. Do not claim visual QA from syntax tests alone.

**Deployment behavior:** `.github/workflows/static.yml` uploads
`./landing` to GitHub Pages **automatically on a push to main**. Therefore
do not merge without the site owner's explicit approval and a reviewed
page-by-page preview: merging changes the public website.

Rollback: revert the website PR in `main` to restore the previous site. No
DNS changes or server restart are required. Do not roll back customer-facing
bot repositories as part of a marketing-site rollback.

## Content governance

The three pages are separate products, not a single subscription or price
list. The shared legal links explicitly identify the current Autoposting
documents; product-specific legal terms must be reviewed before making new
commercial claims. The Social source concept is `Aragova/vimi-social`
`docs/PRODUCT_VISION.md` and `docs/PROJECT_STATUS.md`; Pay claims are
based on `Aragova/telegram-membership-saas` documentation and do not assert
public launch readiness.
