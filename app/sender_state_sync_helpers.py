from __future__ import annotations

from .delivery_idempotency import normalize_valid_sent_message_ids


class SenderStateSyncHelpers:
    def __init__(self, owner):
        self.owner = owner

    def get_rule_intro_items_sync(self, rule):
        return self.get_rule_intro_items(rule)

    def get_rule_intro_items(self, rule):
        horizontal_intro = None
        vertical_intro = None

        horizontal_id = getattr(rule, "video_intro_horizontal_id", None)
        vertical_id = getattr(rule, "video_intro_vertical_id", None)

        try:
            if horizontal_id:
                horizontal_intro = self.owner.db.get_intro_by_id(int(horizontal_id))
        except Exception:
            horizontal_intro = None

        try:
            if vertical_id:
                vertical_intro = self.owner.db.get_intro_by_id(int(vertical_id))
        except Exception:
            vertical_intro = None

        return horizontal_intro, vertical_intro

    def mark_delivery_sent_sync(
        self,
        delivery_id: int,
        *,
        sent_message_id: int | None = None,
        sent_message_ids: list[int] | None = None,
        target_id: str | None = None,
        delivery_method: str | None = None,
    ) -> None:
        if hasattr(self.owner.db, "mark_delivery_sent_with_target_message"):
            self.owner.db.mark_delivery_sent_with_target_message(
                delivery_id,
                sent_message_id=sent_message_id,
                sent_message_ids=sent_message_ids,
                target_id=target_id,
                delivery_method=delivery_method,
            )
            return

        self.owner.db.mark_delivery_sent(delivery_id)

    def mark_many_deliveries_sent_sync(self, delivery_ids: list[int]) -> None:
        self.owner.db.mark_many_deliveries_sent(delivery_ids)

    def mark_album_deliveries_sent_sync(
        self,
        delivery_ids: list[int],
        *,
        sent_message_ids: list[int] | None = None,
        target_id: str | None = None,
        delivery_method: str | None = None,
    ) -> None:
        normalized_delivery_ids = [int(x) for x in (delivery_ids or [])]
        if not normalized_delivery_ids:
            raise RuntimeError("Не удалось определить deliveries альбома для перевода в sent")

        valid_sent_message_ids = normalize_valid_sent_message_ids(sent_message_ids)
        if valid_sent_message_ids and hasattr(self.owner.db, "mark_delivery_sent_with_target_message"):
            for index, album_delivery_id in enumerate(normalized_delivery_ids):
                sent_message_id = valid_sent_message_ids[index] if index < len(valid_sent_message_ids) else valid_sent_message_ids[0]
                self.owner.db.mark_delivery_sent_with_target_message(
                    album_delivery_id,
                    sent_message_id=int(sent_message_id),
                    sent_message_ids=valid_sent_message_ids,
                    target_id=target_id,
                    delivery_method=delivery_method,
                )
            return

        self.owner.db.mark_many_deliveries_sent(normalized_delivery_ids)

    def mark_delivery_faulty_sync(self, delivery_id: int, error_text: str) -> None:
        self.owner.db.mark_delivery_faulty(delivery_id, error_text)

    def get_post_id_by_delivery_sync(self, delivery_id: int) -> int | None:
        return self.owner.db.get_post_id_by_delivery(delivery_id)

    def get_post_id_by_delivery(self, delivery_id: int) -> int | None:
        return self.owner.db.get_post_id_by_delivery(delivery_id)
