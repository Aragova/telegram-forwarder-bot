"""Static deployment contract for the three-product ViMi Elements website.

Uses only stdlib: new marketing pages never depend on the Telegram runtime.
"""
from __future__ import annotations

import re
import unittest
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1] / "landing"
PAGES = {
    "home": ROOT / "index.html",
    "autoposting": ROOT / "autoposting" / "index.html",
    "social": ROOT / "social" / "index.html",
    "pay": ROOT / "pay" / "index.html",
}


class PageParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.attrs = []
        self.ids = set()

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        self.attrs.append((tag, attributes))
        if "id" in attributes:
            self.ids.add(attributes["id"])


class ElementsSiteTest(unittest.TestCase):
    def test_pages_exist_with_correct_product_and_canonical(self):
        for name, path in PAGES.items():
            with self.subTest(page=name):
                html = path.read_text(encoding="utf-8")
                parsed = PageParser()
                parsed.feed(html)
                self.assertTrue(html.lower().startswith("<!doctype html>"))
                self.assertTrue(any(tag == "html" and attrs.get("lang") == "ru"
                                    for tag, attrs in parsed.attrs))
                self.assertTrue(any(tag == "body" and attrs.get("data-page") == name
                                    for tag, attrs in parsed.attrs))
                self.assertEqual(sum(tag == "h1" for tag, _ in parsed.attrs), 1)
                canonical = "https://usevimi.ru/" + (name + "/" if name != "home" else "")
                self.assertTrue(any(tag == "link" and attrs.get("rel") == "canonical"
                                    and attrs.get("href") == canonical
                                    for tag, attrs in parsed.attrs))
                self.assertTrue(any(tag == "meta" and attrs.get("name") == "description"
                                    and attrs.get("content") for tag, attrs in parsed.attrs))
                self.assertTrue(any(tag == "a" and attrs.get("class", "").find("skip-link") >= 0
                                    for tag, attrs in parsed.attrs))
                self.assertIn("main", parsed.ids)
                self.assertTrue(any(tag == "script" and
                                    attrs.get("src", "").endswith("elements.js")
                                    for tag, attrs in parsed.attrs))

    def test_all_local_links_and_assets_resolve(self):
        for name, path in PAGES.items():
            parsed = PageParser()
            parsed.feed(path.read_text(encoding="utf-8"))
            for tag, attrs in parsed.attrs:
                for key in ("href", "src"):
                    href = attrs.get(key, "")
                    if not href or href.startswith(("#", "http:", "https:", "mailto:", "tel:")):
                        if href.startswith("#") and len(href) > 1:
                            self.assertIn(href[1:], parsed.ids, (name, href))
                        continue
                    if href.startswith("data:"):
                        self.fail(f"{name}: inline data URL not expected")
                    local = (path.parent / urlparse(href).path).resolve()
                    self.assertTrue(local.is_relative_to(ROOT), (name, href))
                    if local.is_dir():
                        local /= "index.html"
                    self.assertTrue(local.is_file(), (name, href, str(local)))

    def test_every_visible_string_has_english_translation(self):
        js = (ROOT / "elements.js").read_text(encoding="utf-8")
        english = set(re.findall(r'^\s*"([a-zA-Z][a-zA-Z0-9.]*)"\s*:', js, re.M))
        for page, path in PAGES.items():
            parser = PageParser()
            parser.feed(path.read_text(encoding="utf-8"))
            for _, attrs in parser.attrs:
                if attrs.get("data-i18n"):
                    self.assertIn(attrs["data-i18n"], english, (page, attrs["data-i18n"]))
            self.assertTrue(any(attrs.get("data-lang-choice") == "en"
                                for _, attrs in parser.attrs))
            self.assertTrue(any(attrs.get("data-lang-choice") == "ru"
                                for _, attrs in parser.attrs))

    def test_site_navigation_separates_home_from_full_product_names(self):
        """The ecosystem home link sits above three fully named product tabs."""
        expected = (
            ('autoposting', 'ViMi Autoposting'),
            ('social', 'ViMi Social'),
            ('pay', 'ViMi Pay'),
        )
        for current_page, page in PAGES.items():
            with self.subTest(page=current_page):
                source = page.read_text(encoding="utf-8")
                header = source.split('<header class="site-header">', 1)[1].split('</header>', 1)[0]
                top, product_nav = header.split('<nav id="main-nav"', 1)
                self.assertIn('class="header-primary"', top)
                self.assertIn('class="home-entry"', top)
                self.assertIn('data-i18n="common.home">Главная</a>', top)
                self.assertNotIn('data-i18n="common.home"', product_nav)
                self.assertIn('aria-label="Продукты ViMi"', product_nav)
                self.assertEqual(product_nav.count('data-i18n="common.'), 3)
                self.assertNotIn('menu-toggle', header)
                for slug, title in expected:
                    self.assertIn('data-i18n="common.' + slug + '">' + title + '</a>', product_nav)
                self.assertEqual(product_nav.count('aria-current="page"'), 0 if current_page == 'home' else 1)
                self.assertEqual(top.count('aria-current="page"'), 1 if current_page == 'home' else 0)

        javascript = (ROOT / "elements.js").read_text(encoding="utf-8")
        for slug, title in expected:
            self.assertIn('"common.' + slug + '": "' + title + '"', javascript)
        self.assertNotIn('.menu-toggle', javascript)
        css = (ROOT / "elements.css").read_text(encoding="utf-8")
        self.assertIn("overflow-x:auto", css)
        self.assertIn(".header-primary", css)

    def test_element_labels_are_translated_in_both_languages(self):
        """Keep visible elemental decorations and feature badges bilingual.

        dict.ru is derived from rendered Russian HTML; the default DOM must
        therefore never contain English-only labels without translation keys.
        """
        import html
        js = (ROOT / "elements.js").read_text(encoding="utf-8")
        english = set(re.findall(r'^\s*"([a-zA-Z][a-zA-Z0-9.]*)"\s*:', js, re.M))

        requirements = {
            "home": {
                "home.flowLabel": "ПОТОК",
                "home.energyLabel": "ЭНЕРГИЯ",
                "home.valueLabel": "ЦЕННОСТЬ",
                "home.waterSkyLabel": "ВОДА / НЕБО",
                "home.fireEnergyLabel": "ОГОНЬ / ЭНЕРГИЯ",
                "home.earthValueLabel": "ЗЕМЛЯ / ЦЕННОСТЬ",
            },
            "autoposting": {
                "autoposting.elementLabel": "ВОДА · ViMi",
                "autoposting.ctaElement": "ПОТОК",
            },
            "social": {
                "social.flag1": "AI-поиск",
                "social.flag2": "Подготовка медиа",
                "social.flag3": "Подтверждение человеком",
                "social.ctaElement": "ЭНЕРГИЯ",
            },
            "pay": {
                "pay.ctaElement": "ЦЕННОСТЬ",
            },
        }
        # The new approved hero art replaces former floating labels; the
        # remaining visible card/category labels must still be translated.
        # Product CTA labels are kept as semantic HTML rather than baked into art.
        for page, labels in requirements.items():
            source = PAGES[page].read_text(encoding="utf-8")
            self.assertEqual(source.count('data-i18n="common.brandTagline"'), 2)
            for key, russian in labels.items():
                with self.subTest(page=page, label=key):
                    self.assertIn(key, english, "Missing EN translation")
                    pattern = r'data-i18n="' + re.escape(key) + r'"[^>]*>' + re.escape(html.escape(russian)) + r'<'
                    self.assertRegex(source, pattern)
            for unwanted in ('>Media Preparation<', '>Human Approval<', '>ELEMENTS ECOSYSTEM<'):
                self.assertNotIn(unwanted, source, (page, unwanted))

    def test_product_links_and_status_honesty(self):
        home = PAGES["home"].read_text(encoding="utf-8")
        for product in ("autoposting", "social", "pay"):
            self.assertIn('href="' + product + '/"', home)
        social = PAGES["social"].read_text(encoding="utf-8")
        pay = PAGES["pay"].read_text(encoding="utf-8")
        self.assertIn("проходит тестирование", social)
        self.assertIn("на этапе испытаний", pay)
        self.assertNotIn("t.me/social_bot", social)
        self.assertIn("https://t.me/vimipay_bot", pay)
        self.assertIn("https://t.me/topposter69_bot", PAGES["autoposting"].read_text(encoding="utf-8"))
        # Existing Autoposting SEO page, live links, legal URLs and domain remain.
        self.assertTrue((ROOT / "autoposting.html").is_file())
        self.assertEqual((ROOT / "CNAME").read_text().strip(), "usevimi.ru")

    def test_existing_landing_language_contract_and_legacy_anchors(self):
        js = (ROOT / "elements.js").read_text(encoding="utf-8")
        self.assertIn("const dict = {", js)
        self.assertIn("dict.ru =", js)
        self.assertIn("vimi_lang", js)
        self.assertNotIn("vimi-elements-language", js)
        self.assertIn("autoposting.pricingButton", js)
        new_auto = PAGES["autoposting"].read_text(encoding="utf-8")
        self.assertIn('id="capabilities"', new_auto)
        self.assertIn('id="pricing"', new_auto)
        for old_page in ("autoposting.html", "telegram-bot.html", "video-repost.html"):
            text = (ROOT / old_page).read_text(encoding="utf-8")
            for outdated in ("index.html#features", "index.html#how", "index.html#pricing"):
                self.assertNotIn(outdated, text, old_page)
            self.assertIn("autoposting/#pricing", text, old_page)
            self.assertIn("autoposting/#capabilities", text, old_page)
            self.assertIn("autoposting/#how", text, old_page)

    def test_every_instruction_link_points_to_restored_guide(self):
        homepage = PAGES["home"].read_text(encoding="utf-8")
        self.assertIn('href="autoposting/#how" data-i18n="footer.instructions"', homepage)
        for product in ("autoposting", "social", "pay"):
            page = PAGES[product].read_text(encoding="utf-8")
            target = "#how" if product == "autoposting" else "../autoposting/#how"
            self.assertIn('href="' + target + '" data-i18n="footer.instructions"', page)
        for old_page in ("autoposting.html", "telegram-bot.html", "video-repost.html"):
            text = (ROOT / old_page).read_text(encoding="utf-8")
            self.assertIn('href="autoposting/#how"', text)

    def test_restored_autoposting_includes_full_original_sales_and_help(self):
        """The original site had 6 features, 5 steps, 3 plans and 8 methods."""
        html = PAGES["autoposting"].read_text(encoding="utf-8")
        for section in ("capabilities", "how", "pricing", "faq"):
            self.assertIn('id="' + section + '"', html)
        self.assertEqual(html.count('class="feature auto-feature"'), 6)
        self.assertEqual(html.count('class="auto-step"'), 5)
        self.assertEqual(html.count('class="auto-plan '), 3)
        self.assertEqual(html.count('class="payment-chip"'), 8)
        for name in ("FREE", "BASIC", "PRO"):
            self.assertIn("<h3>" + name + "</h3>", html)
        for value in ("0", "9", "29"):
            self.assertIn('class="auto-price">' + value + "<span>", html)
        self.assertIn('class="auto-demo"', html)
        self.assertIn("https://t.me/topposter69_bot", html)
        self.assertIn('href="#pricing"', html)
        for icon in ("autoposting", "video", "tariffs", "payments", "stats", "automation"):
            self.assertIn('../assets/features/' + icon + '.svg', html)
        for method in ("telegram", "paypal", "card", "sbp", "bitcoin", "tribute", "lava", "manual"):
            self.assertIn('../assets/payments/' + method + '.svg', html)

    def test_approved_element_artwork_is_distinct_and_fully_local(self):
        """Approved element art is stored locally and all CTA actions remain real links."""
        import struct
        artwork = {
            "home": ("vimi-home-art.avif", 720, 477),
            "autoposting": ("vimi-water-art.avif", 620, 795),
            "social": ("vimi-fire-art.avif", 620, 795),
            "pay": ("vimi-earth-art.avif", 620, 795),
        }
        for page, (filename, width, height) in artwork.items():
            with self.subTest(page=page):
                source = PAGES[page].read_text(encoding="utf-8")
                file_path = ROOT / "assets" / filename
                self.assertTrue(file_path.is_file(), str(file_path))
                raw = file_path.read_bytes()
                self.assertGreater(len(raw), 5000)
                self.assertLess(len(raw), 250000, "Avoid high-cost hero images")
                self.assertEqual(raw[4:8], b"ftyp")
                self.assertIn(b"avif", raw[:32])
                self.assertIn(filename, source)
                self.assertIn('class="', source)
                self.assertIn('data-robot="art"', source)
                self.assertIn('fetchpriority="high"', source)
                self.assertIn('data-i18n=', source)
                if page == "home":
                    for route in ('autoposting/', 'social/', 'pay/'):
                        self.assertIn('class="element-entry', source)
                        self.assertIn('href="' + route + '"', source)
                else:
                    self.assertIn('class="product-stage robot-stage', source)
                    self.assertNotIn('src="../assets/hero-robot-ru.png"', source)
        home = PAGES["home"].read_text(encoding="utf-8")
        for filename in ("vimi-water-art.avif", "vimi-fire-art.avif", "vimi-earth-art.avif"):
            self.assertIn(filename, home)
        for old in ("assets/hero-robot-ru.png", 'class="sculpture water"'):
            self.assertNotIn(old, home)
        css = (ROOT / "elements.css").read_text(encoding="utf-8")
        self.assertIn(".element-art-stage", css)
        self.assertIn(".element-artwork", css)
        self.assertIn("body[data-product=social]", css)
        self.assertIn("body[data-product=pay]", css)
        self.assertIn("@media(max-width:430px)", css)

    def test_original_vimi_robot_and_strong_elemental_backgrounds(self):
        """Never replace the brand's character with generic three-dimensional orbs."""
        script = (ROOT / "elements.js").read_text(encoding="utf-8")
        css = (ROOT / "elements.css").read_text(encoding="utf-8")
        for page, html_file in PAGES.items():
            html = html_file.read_text(encoding="utf-8")
            self.assertIn('data-robot=', html, page)
            self.assertIn('class="vimi-robot', html, page)
            self.assertIn('family=Inter:wght@', html, page)
            self.assertIn('family=Manrope:wght@', html, page)
        for variant in ("hero-robot-ru.png", "hero-robot-en.png",
                        "hero-scene-ru.png", "hero-scene-en.png"):
            self.assertTrue((ROOT / "assets" / variant).is_file(), variant)
        for product in ("autoposting", "social", "pay"):
            self.assertIn("body[data-product=" + product + "]", css)
        self.assertIn("--product-rgb", css)
        self.assertIn("font-family:Inter", css)
        self.assertIn("[data-robot]", script)
        self.assertIn("current + \".png\"", script)

    def test_hero_copy_is_concise(self):
        for page, path in PAGES.items():
            source = path.read_text(encoding="utf-8")
            hero = source.split('<section class="hero', 1)[1].split('</section>', 1)[0]
            match = re.search(r'<p class="hero-lead"[^>]*>([^<]+)</p>', hero)
            self.assertIsNotNone(match, page)
            self.assertLessEqual(len(match.group(1)), 125, page)

    def test_responsive_reduced_motion_and_no_heavy_external_runtime(self):
        css = (ROOT / "elements.css").read_text(encoding="utf-8")
        js = (ROOT / "elements.js").read_text(encoding="utf-8")
        self.assertIn("prefers-reduced-motion", css)
        self.assertIn("@media (max-width:780px)", css)
        self.assertIn("focus-visible", css)
        self.assertIn("pointermove", js)
        self.assertIn("requestAnimationFrame", js)
        self.assertIn("localStorage", js)
        self.assertNotIn("eval(", js)
        self.assertNotIn("three.min.js", js)
        self.assertNotIn("cdnjs", css + js)

    def test_mobile_product_hero_hierarchy_is_compact(self):
        """Keep first-screen mascot, CTA and status coherent at phone widths."""
        css = (ROOT / "elements.css").read_text(encoding="utf-8")
        marker = "Mobile hero correction after visual QA on /pay/"
        self.assertEqual(css.count(marker), 1)
        mobile = css.split(marker, 1)[1]
        for selector in (
            ".product-hero .hero-copy{display:contents}",
            ".product-hero .tag{grid-column:1;grid-row:1",
            ".product-hero h1{grid-column:1;grid-row:2",
            ".product-hero .hero-lead{grid-column:1;grid-row:3",
            ".product-hero .actions{grid-column:1;grid-row:4",
            ".robot-stage.element-art-stage{",
            "grid-column:1;grid-row:5",
            ".product-hero .product-note{",
            "grid-column:1;grid-row:6",
            ".product-hero .product-flags{display:none}",
            "min-height:0;max-height:none",
            ".nav-links{min-height:38px",
        ):
            self.assertIn(selector, mobile, selector)
        for product in ("autoposting", "social", "pay"):
            source = PAGES[product].read_text(encoding="utf-8")
            self.assertIn('class="hero product-hero"', source)
            self.assertIn('class="product-stage robot-stage element-art-stage"', source)
            self.assertIn('class="actions"', source)
            self.assertIn('data-robot="art"', source)

    def test_sitemap_contains_all_three_product_routes(self):
        import xml.etree.ElementTree as ET
        sitemap = ET.fromstring((ROOT / "sitemap.xml").read_text(encoding="utf-8"))
        locs = {node.text for node in sitemap.iter() if node.tag.endswith("loc")}
        for name in ("autoposting", "social", "pay"):
            self.assertIn(f"https://usevimi.ru/{name}/", locs)


if __name__ == "__main__":
    unittest.main()
