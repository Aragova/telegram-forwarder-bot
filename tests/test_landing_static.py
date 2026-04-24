from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "landing"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_landing_files_exist():
    assert (LANDING / "index.html").exists()


def test_ru_legal_pages_exist():
    for name in ("terms.html", "privacy.html", "refund.html", "contacts.html"):
        assert (LANDING / "ru" / name).exists()


def test_en_legal_pages_exist():
    for name in ("terms.html", "privacy.html", "refund.html", "contacts.html"):
        assert (LANDING / "en" / name).exists()


def test_instructions_pages_exist():
    assert (LANDING / "ru" / "instructions.html").exists()
    assert (LANDING / "en" / "instructions.html").exists()


def test_index_contains_bot_cta_and_legal_links():
    index_text = read(LANDING / "index.html")
    app_text = read(LANDING / "app.js")
    assert "cta-open-bot" in index_text
    assert "botUrl" in app_text
    for link_id in ("link-terms", "link-privacy", "link-refund", "link-contacts", "link-help"):
        assert link_id in index_text


def test_legal_disclaimer_present_ru_en():
    ru_disclaimer = "Этот документ является шаблоном и не является юридической консультацией"
    en_disclaimer = "This document is a template and does not constitute legal advice"
    for path in (LANDING / "ru").glob("*.html"):
        if path.name != "instructions.html":
            assert ru_disclaimer in read(path)
    for path in (LANDING / "en").glob("*.html"):
        if path.name != "instructions.html":
            assert en_disclaimer in read(path)


def test_owner_plan_not_visible():
    index_text = read(LANDING / "index.html") + read(LANDING / "app.js")
    assert "OWNER" not in index_text


def test_payment_honesty_statement_exists_ru_en():
    text = read(LANDING / "app.js")
    assert "Некоторые способы оплаты могут работать автоматически, а некоторые — через ручное подтверждение администратором." in text
    assert "Some payment methods may work automatically, while others may require manual administrator confirmation." in text


def test_internal_terms_not_exposed_on_landing():
    joined = "\n".join(read(path) for path in LANDING.rglob("*.html")) + read(LANDING / "app.js")
    for bad in ("worker", "tenant_id", "dedup", "lease", "jobs"):
        assert bad not in joined.lower()
