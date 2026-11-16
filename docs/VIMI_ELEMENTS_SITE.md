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
  `elements.js`. A visitor can switch languages without leaving the product
  page; preference is saved only in localStorage when supported.
- Lightweight CSS transforms and gradients replace WebGL. Pointer effects
  are disabled for touch/non-hover devices, and all animation is disabled by
  `prefers-reduced-motion`. All links/buttons are keyboard focusable.
- Dedicated canonical URLs and existing social preview image are retained.
  Sitemap includes the three product pages.
- Existing older Autoposting SEO page `/autoposting.html`, all `/ru/` and
  `/en/` legal pages, media assets, `CNAME` and old `app.js`/`styles.css`
  are left untouched.
- Existing Telegram destinations: Autoposting `@topposter69_bot`, Pay
  `@vimipay_bot`; Social **does not invent a public bot** and instead links
  to existing `@vimi_support_bot`. Status notices avoid fictional product
  launch dates, pricing or payment guarantees.

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
