import importlib
from pathlib import Path


FOUNDATION_MODULES = (
    "app.reaction_post_send_service",
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
