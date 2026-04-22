from __future__ import annotations

from app.postgres_repository import PostgresRepository


def main() -> None:
    repo = PostgresRepository()
    repo.init()
    print("OK: PostgreSQL schema initialized")


if __name__ == "__main__":
    main()
