"""Tenant-scoped rule folder repository. No scheduler or delivery state changes."""
from __future__ import annotations

import re
from collections.abc import Collection

from app.rule_catalog import CATALOG_PAGE_SIZE, RuleListItem, RulePage


def normalize_folder_name(value: str) -> str:
    name = re.sub(r"\s+", " ", str(value or "")).strip()
    if not name or len(name) > 64:
        raise ValueError("Название папки должно содержать от 1 до 64 символов")
    return name


class RuleFoldersRepositoryMixin:
    def count_catalog_rules(self, tenant_id: int | None = None) -> int:
        """Count rules without loading routing objects or channel titles."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                if tenant_id is None:
                    cur.execute("SELECT COUNT(*) AS cnt FROM routing")
                else:
                    cur.execute(
                        "SELECT COUNT(*) AS cnt FROM routing WHERE COALESCE(tenant_id, 1) = %s",
                        (int(tenant_id),),
                    )
                return int(cur.fetchone()["cnt"])

    def get_rule_catalog_page(
        self, tenant_id: int | None = None, *,
        folder_tenant_id: int | None = None,
        owner_admin_id: int | None = None,
        folder_id: int = -1,
        exclude_folder_id: int | None = None,
        search: str | None = None,
        active: bool | None = None,
        page: int = 0,
        page_size: int = CATALOG_PAGE_SIZE,
        selected_ids: Collection[int] = (),
    ) -> RulePage:
        """Return one page and exact scoped counts from the same SQL snapshot.

        tenant_id=None is the system-admin global catalog. Legacy owner access
        is explicit and limited to the workspace used for folder management.
        Folder assignments always belong to the actor's tenant, never another
        tenant. All filtering precedes pagination.
        """
        if not 1 <= page_size <= 100:
            raise ValueError("Некорректный размер страницы")
        if owner_admin_id is not None and tenant_id is None:
            raise ValueError("Для управления папками нужен аккаунт")
        if folder_tenant_id is None:
            folder_tenant_id = tenant_id
        if tenant_id is not None and folder_tenant_id != tenant_id:
            raise ValueError("Некорректный аккаунт папок")
        if (folder_id >= 0 or exclude_folder_id is not None) and folder_tenant_id is None:
            raise ValueError("Для фильтра папок нужен аккаунт")
        # Callback page numbers are untrusted and may exceed BIGINT.
        requested_page = min(max(0, int(page)), 2**31 - 1)
        filters = ["TRUE"]
        params = [folder_tenant_id]
        if tenant_id is not None:
            if owner_admin_id is None:
                filters.append("COALESCE(r.tenant_id, 1) = %s")
                params.append(int(tenant_id))
            else:
                filters.append("(COALESCE(r.tenant_id, 1) = %s OR r.created_by = %s)")
                params.extend((int(tenant_id), int(owner_admin_id)))
        if folder_id >= 0:
            filters.append("COALESCE(f.id, 0) = %s")
            params.append(int(folder_id))
        if exclude_folder_id is not None:
            filters.append("COALESCE(f.id, 0) <> %s")
            params.append(int(exclude_folder_id))
        if active is not None:
            filters.append("r.is_active = %s")
            params.append(bool(active))
        if search is not None:
            needle = str(search).strip()
            exact = re.fullmatch(r"#\s*(\d+)", needle)
            if not needle:
                filters.append("FALSE")
            elif exact:
                filters.append("r.id::text = %s")
                params.append(str(int(exact.group(1))))
            else:
                # strpos treats % and _ literally, unlike a LIKE pattern.
                filters.append("""
                    (strpos(lower(r.id::text), lower(%s)) > 0
                     OR strpos(lower(COALESCE(NULLIF(s.title, ''), r.source_id)), lower(%s)) > 0
                     OR strpos(lower(COALESCE(NULLIF(t.title, ''), r.target_id)), lower(%s)) > 0
                     OR strpos(lower(r.source_id), lower(%s)) > 0
                     OR strpos(lower(r.target_id), lower(%s)) > 0)
                """)
                params.extend([needle] * 5)
        selected = sorted({int(rid) for rid in selected_ids if 0 < int(rid) < 2**63})
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    WITH eligible AS MATERIALIZED (
                        SELECT r.id, r.created_date, r.source_id, r.source_thread_id,
                               r.target_id, r.target_thread_id, r.interval, r.is_active,
                               s.title AS source_title, t.title AS target_title,
                               f.id AS folder_id, f.name AS folder_name
                        FROM routing r
                        LEFT JOIN channels s
                          ON s.channel_id = r.source_id
                         AND s.thread_id IS NOT DISTINCT FROM r.source_thread_id
                         AND s.channel_type = 'source'
                         AND COALESCE(s.tenant_id, 1) = COALESCE(r.tenant_id, 1)
                        LEFT JOIN channels t
                          ON t.channel_id = r.target_id
                         AND t.thread_id IS NOT DISTINCT FROM r.target_thread_id
                         AND t.channel_type = 'target'
                         AND COALESCE(t.tenant_id, 1) = COALESCE(r.tenant_id, 1)
                        LEFT JOIN rule_folder_assignments a
                          ON a.rule_id = r.id AND a.tenant_id = %s
                        LEFT JOIN rule_folders f
                          ON f.id = a.folder_id AND f.tenant_id = a.tenant_id
                        WHERE {" AND ".join(filters)}
                    ), stats AS (
                        SELECT COUNT(*) AS total,
                               COUNT(*) FILTER (WHERE is_active) AS active,
                               COALESCE(array_agg(id) FILTER (WHERE id = ANY(%s::bigint[])),
                                        ARRAY[]::bigint[]) AS valid_selected_ids
                        FROM eligible
                    ), bounds AS (
                        SELECT *, LEAST(%s::bigint, GREATEST(total - 1, 0) / %s) AS page
                        FROM stats
                    )
                    SELECT bounds.*, e.*
                    FROM bounds LEFT JOIN LATERAL (
                        SELECT * FROM eligible ORDER BY created_date, id
                        LIMIT %s OFFSET (bounds.page * %s)
                    ) e ON TRUE
                    ORDER BY e.created_date, e.id
                    """,
                    (*params, selected, requested_page, page_size, page_size, page_size),
                )
                rows = cur.fetchall()
        stats = rows[0]
        rules = []
        assignments = {}
        for row in rows:
            if row["id"] is None:
                continue
            rules.append(RuleListItem(
                id=int(row["id"]), source_id=row["source_id"],
                source_thread_id=row["source_thread_id"], target_id=row["target_id"],
                target_thread_id=row["target_thread_id"], interval=int(row["interval"]),
                is_active=bool(row["is_active"]), source_title=row["source_title"],
                target_title=row["target_title"],
            ))
            if row["folder_id"] is not None:
                assignments[int(row["id"])] = {
                    "id": int(row["folder_id"]), "name": row["folder_name"],
                }
        return RulePage(
            rules=rules, total=int(stats["total"]), active=int(stats["active"]),
            page=int(stats["page"]), page_size=page_size, assignments=assignments,
            valid_selected_ids=set(stats["valid_selected_ids"]),
        )

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

    def get_rule_folder_map(self, tenant_id: int, rule_ids: Collection[int] | None = None) -> dict[int, dict]:
        """Read all assignments only when explicitly requested; card lookups use IDs."""
        params = [int(tenant_id)]
        predicate = ""
        if rule_ids is not None:
            ids = sorted({int(rid) for rid in rule_ids})
            if not ids:
                return {}
            predicate = "AND a.rule_id = ANY(%s)"
            params.append(ids)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT a.rule_id, f.id AS folder_id, f.name
                    FROM rule_folder_assignments a
                    JOIN rule_folders f ON f.id = a.folder_id AND f.tenant_id = a.tenant_id
                    JOIN routing r ON r.id = a.rule_id
                    WHERE a.tenant_id = %s {predicate}
                    """,
                    tuple(params),
                )
                return {
                    int(row["rule_id"]): {"id": int(row["folder_id"]), "name": str(row["name"])}
                    for row in cur.fetchall()
                }

    def can_manage_folder_rule(self, tenant_id: int, owner_admin_id: int, rule_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM routing
                        WHERE id = %s AND (COALESCE(tenant_id, 1) = %s OR created_by = %s)
                    ) AS allowed
                    """,
                    (int(rule_id), int(tenant_id), int(owner_admin_id)),
                )
                return bool(cur.fetchone()["allowed"])

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
