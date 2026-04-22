from __future__ import annotations

from app.db import Database
from app.postgres_client import PostgresClient


def sqlite_counts(db: Database) -> dict[str, int]:
    with db.connect() as conn:
        return {
            "channels": int(conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0]),
            "posts": int(conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]),
            "routing": int(conn.execute("SELECT COUNT(*) FROM routing").fetchone()[0]),
            "intros": int(conn.execute("SELECT COUNT(*) FROM intros").fetchone()[0]),
            "deliveries": int(conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]),
            "problem_state": int(conn.execute("SELECT COUNT(*) FROM problem_state").fetchone()[0]),
            "audit_log": int(conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]),
        }


def postgres_counts(pg: PostgresClient) -> dict[str, int]:
    row = pg.fetchone("""
        SELECT
            (SELECT COUNT(*) FROM channels) AS channels,
            (SELECT COUNT(*) FROM posts) AS posts,
            (SELECT COUNT(*) FROM routing) AS routing,
            (SELECT COUNT(*) FROM intros) AS intros,
            (SELECT COUNT(*) FROM deliveries) AS deliveries,
            (SELECT COUNT(*) FROM problem_state) AS problem_state,
            (SELECT COUNT(*) FROM audit_log) AS audit_log
    """)

    return {
        "channels": int(row["channels"] or 0),
        "posts": int(row["posts"] or 0),
        "routing": int(row["routing"] or 0),
        "intros": int(row["intros"] or 0),
        "deliveries": int(row["deliveries"] or 0),
        "problem_state": int(row["problem_state"] or 0),
        "audit_log": int(row["audit_log"] or 0),
    }


def main() -> None:
    sqlite_db = Database()
    pg = PostgresClient()

    left = sqlite_counts(sqlite_db)
    right = postgres_counts(pg)

    print("SQLITE_COUNTS:")
    for k, v in left.items():
        print(f"  {k}: {v}")

    print("POSTGRES_COUNTS:")
    for k, v in right.items():
        print(f"  {k}: {v}")

    print("DIFF:")
    for k in left:
        diff = right[k] - left[k]
        print(f"  {k}: {diff}")


if __name__ == "__main__":
    main()
