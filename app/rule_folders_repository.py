"""Tenant-scoped rule folder repository. No scheduler or delivery state changes."""
from __future__ import annotations

import re


def normalize_folder_name(value: str) -> str:
    name = re.sub(r"\s+", " ", str(value or "")).strip()
    if not name or len(name) > 64:
        raise ValueError("Название папки должно содержать от 1 до 64 символов")
    return name


class RuleFoldersRepositoryMixin:
    def list_rule_folders(self, tenant_id: int) -> list[dict]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT f.id, f.tenant_id, f.name,
                           COUNT(r.id)::int AS rules_count,
                           COUNT(r.id) FILTER (WHERE r.is_active)::int AS active_count
                    FROM rule_folders f
                    LEFT JOIN rule_folder_assignments a
                      ON a.folder_id = f.id AND a.tenant_id = f.tenant_id
                    LEFT JOIN routing r ON r.id = a.rule_id
                    WHERE f.tenant_id = %s
                    GROUP BY f.id, f.tenant_id, f.name
                    ORDER BY lower(f.name), f.id
                    """,
                    (int(tenant_id),),
                )
                return [dict(row) for row in cur.fetchall()]

    def get_rule_folder(self, tenant_id: int, folder_id: int) -> dict | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, tenant_id, name FROM rule_folders WHERE id = %s AND tenant_id = %s",
                    (int(folder_id), int(tenant_id)),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def create_rule_folder(self, tenant_id: int, name: str) -> int | None:
        name = normalize_folder_name(name)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO rule_folders (tenant_id, name)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (int(tenant_id), name),
                )
                row = cur.fetchone()
            conn.commit()
        return int(row["id"]) if row else None

    def rename_rule_folder(self, tenant_id: int, folder_id: int, name: str) -> bool:
        name = normalize_folder_name(name)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE rule_folders SET name = %s, updated_at = NOW()
                    WHERE id = %s AND tenant_id = %s
                      AND NOT EXISTS (
                        SELECT 1 FROM rule_folders other
                        WHERE other.tenant_id = %s AND other.id <> %s
                          AND lower(other.name) = lower(%s)
                      )
                    """,
                    (name, int(folder_id), int(tenant_id),
                     int(tenant_id), int(folder_id), name),
                )
                updated = cur.rowcount == 1
            conn.commit()
        return updated

    def delete_rule_folder(self, tenant_id: int, folder_id: int) -> bool:
        # Assignments are deleted by FK CASCADE; rules and deliveries are untouched.
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM rule_folders WHERE id = %s AND tenant_id = %s",
                    (int(folder_id), int(tenant_id)),
                )
                deleted = cur.rowcount == 1
            conn.commit()
        return deleted

    def get_rule_folder_map(self, tenant_id: int) -> dict[int, dict]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT a.rule_id, f.id AS folder_id, f.name
                    FROM rule_folder_assignments a
                    JOIN rule_folders f ON f.id = a.folder_id AND f.tenant_id = a.tenant_id
                    JOIN routing r ON r.id = a.rule_id
                    WHERE a.tenant_id = %s
                    """,
                    (int(tenant_id),),
                )
                return {
                    int(row["rule_id"]): {"id": int(row["folder_id"]), "name": str(row["name"])}
                    for row in cur.fetchall()
                }

    def list_folder_manageable_rule_ids(
        self, tenant_id: int, owner_admin_id: int,
    ) -> set[int]:
        """Owner's workspace includes legacy rules created by them in tenant 1.

        No change to rule.tenant_id or to the SaaS user's ordinary tenant ACL.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id FROM routing
                    WHERE COALESCE(tenant_id, 1) = %s OR created_by = %s
                    """,
                    (int(tenant_id), int(owner_admin_id)),
                )
                return {int(row["id"]) for row in cur.fetchall()}

    def assign_rules_to_folder(
        self, tenant_id: int, rule_ids: list[int], folder_id: int | None,
        *, owner_admin_id: int | None = None,
    ) -> int:
        """All-or-nothing tenant-safe bulk move, including the 'unfiled' case."""
        ids = list(dict.fromkeys(int(x) for x in rule_ids))
        if not ids:
            return 0
        with self.connect() as conn:
            with conn.cursor() as cur:
                if folder_id is not None:
                    cur.execute(
                        "SELECT id FROM rule_folders WHERE tenant_id = %s AND id = %s FOR UPDATE",
                        (int(tenant_id), int(folder_id)),
                    )
                    if not cur.fetchone():
                        raise ValueError("Папка не найдена")
                where = (
                    "id = ANY(%s) AND COALESCE(tenant_id, 1) = %s"
                    if owner_admin_id is None
                    else "id = ANY(%s) AND (COALESCE(tenant_id, 1) = %s OR created_by = %s)"
                )
                params = (
                    (ids, int(tenant_id))
                    if owner_admin_id is None
                    else (ids, int(tenant_id), int(owner_admin_id))
                )
                cur.execute(
                    f"SELECT COUNT(*)::int AS cnt FROM routing WHERE {where}",
                    params,
                )
                if int(cur.fetchone()["cnt"]) != len(ids):
                    raise ValueError("Одно или несколько правил не принадлежат аккаунту")
                # An existing assignment of another owner must never be stolen.
                cur.execute(
                    """
                    SELECT COUNT(*)::int AS cnt FROM rule_folder_assignments
                    WHERE rule_id = ANY(%s) AND tenant_id <> %s
                    """,
                    (ids, int(tenant_id)),
                )
                if int(cur.fetchone()["cnt"]) != 0:
                    raise ValueError("Одно из правил находится в папке другого аккаунта")
                if folder_id is None:
                    cur.execute(
                        "DELETE FROM rule_folder_assignments WHERE tenant_id = %s AND rule_id = ANY(%s)",
                        (int(tenant_id), ids),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO rule_folder_assignments (rule_id, tenant_id, folder_id)
                        SELECT id, %s, %s FROM routing
                        WHERE {where}
                        ON CONFLICT (rule_id)
                        DO UPDATE SET folder_id = EXCLUDED.folder_id,
                                      tenant_id = EXCLUDED.tenant_id
                        WHERE rule_folder_assignments.tenant_id = EXCLUDED.tenant_id
                        """.format(where=where),
                        (int(tenant_id), int(folder_id), *params),
                    )
                    if cur.rowcount != len(ids):
                        raise ValueError("Некоторые правила уже находятся в папке другого аккаунта")
            conn.commit()
        return len(ids)
