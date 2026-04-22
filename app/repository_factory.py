from app.postgres_repository import PostgresRepository
from app.repository import RepositoryProtocol


def create_repository() -> RepositoryProtocol:
    return PostgresRepository()
