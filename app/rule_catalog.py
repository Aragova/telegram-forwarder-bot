"""Lightweight rule catalog contract shared by PostgreSQL and the UI."""
from __future__ import annotations

from dataclasses import dataclass, field


CATALOG_PAGE_SIZE = 10


@dataclass(frozen=True, slots=True)
class RuleListItem:
    id: int
    source_id: str
    source_thread_id: int | None
    target_id: str
    target_thread_id: int | None
    interval: int
    is_active: bool
    source_title: str | None
    target_title: str | None


@dataclass(slots=True)
class RulePage:
    rules: list[RuleListItem]
    total: int
    active: int
    page: int
    page_size: int
    assignments: dict[int, dict] = field(default_factory=dict)
    valid_selected_ids: set[int] = field(default_factory=set)

    @property
    def pages(self) -> int:
        return max(1, (self.total + self.page_size - 1) // self.page_size)
