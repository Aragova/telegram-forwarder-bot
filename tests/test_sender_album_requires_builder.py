from types import SimpleNamespace

from app.sender import SenderService


def _build_sender_for_album_requires_builder(post_rows_by_message_id):
    sender = SenderService.__new__(SenderService)

    sender._get_post_row_for_rule_message = (
        lambda rule, source_channel, message_id: post_rows_by_message_id.get(int(message_id))
    )
    sender._content_from_message_or_post = (
        lambda message=None, post_row=None: {"has_entities": bool(post_row and post_row.get("has_entities"))}
    )
    sender._content_requires_builder = lambda content: bool(content.get("has_entities"))

    return sender


def test_album_requires_builder_returns_bool():
    sender = _build_sender_for_album_requires_builder(
        {
            31: {"has_entities": False},
            32: {"has_entities": True},
            33: {"has_entities": False},
        }
    )
    rule = SimpleNamespace(id=89)

    result = sender._album_requires_builder(rule, "source_chan", [31, 32, 33])

    assert isinstance(result, bool)
    assert result is True


def test_resolve_repost_caption_delivery_strategy_album_requires_builder_bool():
    sender = _build_sender_for_album_requires_builder(
        {
            31: {"has_entities": False},
            32: {"has_entities": True},
            33: {"has_entities": False},
        }
    )
    rule = SimpleNamespace(id=89, mode="repost", caption_delivery_mode="auto")

    strategy = sender._resolve_repost_caption_delivery_strategy(
        rule=rule,
        source_channel="source_chan",
        message_ids=[31, 32, 33],
        is_album=True,
    )

    assert isinstance(strategy, dict)
    assert isinstance(strategy.get("requires_builder"), bool)
    assert strategy["requires_builder"] is True
    assert strategy["selected_path"] == "builder_first"
    assert strategy["use_copy_first"] is False
