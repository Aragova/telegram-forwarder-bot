from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "landing"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_landing_files_exist():
    assert (LANDING / "index.html").exists()
    assert (LANDING / "styles.css").exists()
    assert (LANDING / "app.js").exists()


def test_brand_and_old_brand_checks():
    joined = read(LANDING / "index.html") + read(LANDING / "app.js")
    assert "ViMi" in joined
    assert "ChannelPilot" not in joined


def test_owner_plan_not_visible():
    joined = read(LANDING / "index.html") + read(LANDING / "app.js")
    assert "OWNER" not in joined


def test_cta_ru_en_and_telegram_bot_present():
    index_text = read(LANDING / "index.html")
    app_text = read(LANDING / "app.js")

    assert "header-open-bot" in index_text
    assert "https://t.me/topposter69_bot" in index_text
    assert "botUrl" in app_text
    assert "Telegram" in app_text
    assert 'data-lang="ru"' in index_text
    assert 'data-lang="en"' in index_text


def test_pricing_and_payment_blocks_exist():
    index_text = read(LANDING / "index.html") + read(LANDING / "app.js")
    assert "plans" in index_text.lower()
    assert "payments" in index_text.lower()


def test_honest_manual_confirmation_phrase_ru_en_exists():
    text = read(LANDING / "app.js")
    assert "Некоторые способы оплаты работают автоматически, некоторые — через ручное подтверждение." in text
    assert "Some payment methods work automatically, while others may require manual confirmation." in text


def test_internal_terms_not_exposed_on_landing():
    joined = "\n".join(read(path) for path in LANDING.rglob("*.html")) + read(LANDING / "app.js")
    for bad in ("worker", "tenant_id", "dedup", "lease", "jobs"):
        assert bad not in joined.lower()


def test_assets_robot_logo_favicon_exist():
    required_assets = (
        "vimi-logo.svg",
        "vimi-robot.svg",
        "vimi-robot-hero.svg",
        "vimi-robot-small.svg",
        "favicon.svg",
    )
    for asset in required_assets:
        assert (LANDING / "assets" / asset).exists()


def test_legal_links_and_faq_exist():
    index_text = read(LANDING / "index.html")
    app_text = read(LANDING / "app.js")

    for link_id in (
        "footer-link-terms",
        "footer-link-privacy",
        "footer-link-refund",
        "footer-link-contacts",
    ):
        assert link_id in index_text

    assert "ru/instructions.html" in index_text or "#how" in index_text
    assert "nav_instructions" in app_text


def test_ru_en_legal_and_instruction_pages_exist():
    for lang in ("ru", "en"):
        for name in ("terms.html", "privacy.html", "refund.html", "contacts.html", "instructions.html"):
            assert (LANDING / lang / name).exists()
