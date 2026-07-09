from pathlib import Path
from types import SimpleNamespace


def test_extracted_runtime_modules_do_not_import_sender():
    runtime_files = [
        "app/repost_single_delivery.py",
        "app/repost_album_delivery.py",
        "app/video_send_delivery.py",
        "app/video_single_delivery.py",
        "app/video_pipeline_stages.py",
    ]

    for path in runtime_files:
        source = Path(path).read_text(encoding="utf-8")
        assert "from ." + "sender import" not in source
        assert "import app." + "sender" not in source
        assert 'import_module("app.sender")' not in source
        assert "import_module('app.sender')" not in source


def test_sender_shared_primitives_extracted():
    sender_source = Path("app/sender.py").read_text(encoding="utf-8")
    runtime_source = Path("app/runtime_utils.py").read_text(encoding="utf-8")
    primitives_source = Path("app/sender_primitives.py").read_text(encoding="utf-8")

    assert "async def run_db(" not in sender_source
    assert "async def run_db(" in runtime_source

    for needle in [
        "def _detect_message_media_kind(",
        "def _prepare_html_text(",
        "def _normalize_source_text(",
        "def _utf16_text_length(",
        "def _normalize_reaction_emoji(",
    ]:
        assert needle not in sender_source
        assert needle in primitives_source

    for needle in [
        "MAX_INVALID_MP4_RETRY",
        "MAX_NORMAL_REACTION_ATTEMPTS",
        "REACTION_POOL",
        "NORMAL_REACTION_POOL",
    ]:
        assert needle in primitives_source


def test_sender_reexports_shared_primitives_for_compatibility():
    import importlib
    sender = importlib.import_module("app." + "sender")

    assert callable(sender.run_db)
    assert callable(sender._detect_message_media_kind)
    assert callable(sender._prepare_html_text)
    assert isinstance(sender.MAX_INVALID_MP4_RETRY, int)
    assert isinstance(sender.NORMAL_REACTION_POOL, list)


def test_detect_message_media_kind_text():
    from app.sender_primitives import _detect_message_media_kind

    assert _detect_message_media_kind(SimpleNamespace(media=None)) == "text"


def test_prepare_html_text_preserves_basic_link():
    from app.sender_primitives import _prepare_html_text

    assert (
        _prepare_html_text("[Example](https://example.com)")
        == '<a href="https://example.com">Example</a>'
    )


def test_normalize_reaction_emoji_removes_variation_selector():
    from app.sender_primitives import _normalize_reaction_emoji

    assert _normalize_reaction_emoji("❤️") == "❤"
