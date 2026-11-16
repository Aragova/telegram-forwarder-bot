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

    def test_element_labels_are_translated_in_both_languages(self):
        """Keep visible elemental decorations and feature badges bilingual.

        dict.ru is derived from rendered Russian HTML; the default DOM must
        therefore never contain English-only labels without translation keys.
        """
        import html
        js = (ROOT / "elements.js").read_text(encoding="utf-8")
        english = set(re.findall(r'^\\s*"([a-zA-Z][a-zA-Z0-9.]*)"\\s*:', js, re.M))

        requirements = {
            "home": {
                "home.flowLabel": "ПОТОК",
                "home.energyLabel": "ЭНЕРГИЯ",
                "home.valueLabel": "ЦЕННОСТЬ",
                "home.waterLabel": "01 · ВОДА",
                "home.fireLabel": "02 · ОГОНЬ",
                "home.earthLabel": "03 · ЗЕМЛЯ",
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
                "social.elementLabel": "ОГОНЬ · ViMi",
                "social.ctaElement": "ЭНЕРГИЯ",
            },
            "pay": {
                "pay.elementLabel": "ЗЕМЛЯ · ViMi",
                "pay.ctaElement": "ЦЕННОСТЬ",
            },
        }
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
            self.assertIn("ru/instructions.html", text, old_page)

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

    def test_sitemap_contains_all_three_product_routes(self):
        import xml.etree.ElementTree as ET
        sitemap = ET.fromstring((ROOT / "sitemap.xml").read_text(encoding="utf-8"))
        locs = {node.text for node in sitemap.iter() if node.tag.endswith("loc")}
        for name in ("autoposting", "social", "pay"):
            self.assertIn(f"https://usevimi.ru/{name}/", locs)


if __name__ == "__main__":
    unittest.main()
