import importlib
from pathlib import Path


FOUNDATION_MODULES = (
    "app.telegram_send_gateway",
    "app.target_verifier",
    "app.attempt_ledger_service",
    "app.post_send_steps",
    "app.delivery_finalizer",
    "app.repost_single_pipeline",
    "app.repost_album_pipeline",
    "app.video_send_pipeline",
    "app.legacy_video_delivery_pipeline",
    "app.repost_campaign_pipeline",
    "app.reaction_post_send_service",
    "app.sender_pipeline_facade",
)


FORBIDDEN_REPOSITORY_IMPORT_MARKERS = (
    "PostgresRepository",
    "from app.postgres",
    "import app.postgres",
)


def test_foundation_modules_do_not_import_concrete_postgres_repository():
    for module_name in FOUNDATION_MODULES:
        module = importlib.import_module(module_name)
        source = Path(module.__file__).read_text(encoding="utf-8")

        for marker in FORBIDDEN_REPOSITORY_IMPORT_MARKERS:
            assert marker not in source, f"{module_name} must not import concrete repository via {marker!r}"
