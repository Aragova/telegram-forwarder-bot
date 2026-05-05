from types import SimpleNamespace

from app.saved_posts_service import (
    build_saved_post_content_from_aiogram_message,
    get_saved_post_preview_caption,
    get_saved_post_short_description,
    summarize_aiogram_message_for_saved_post,
    summarize_saved_post_entities,
)


def test_build_content_text_contains_entities():
    message = SimpleNamespace(
        text="Привет",
        caption=None,
        entities=[SimpleNamespace(type="bold", offset=0, length=6)],
        caption_entities=None,
        photo=None,
        video=None,
        animation=None,
        document=None,
        chat=SimpleNamespace(id=-1001),
        message_id=10,
    )
    content = build_saved_post_content_from_aiogram_message(message)
    assert content["kind"] == "text"
    assert content["text"] == "Привет"
    assert content["entities"][0]["type"] == "bold"


def test_build_content_photo_contains_file_and_caption_entities():
    message = SimpleNamespace(
        text=None,
        caption="Подпись",
        entities=None,
        caption_entities=[SimpleNamespace(type="custom_emoji", offset=0, length=1, custom_emoji_id="777")],
        photo=[SimpleNamespace(file_id="small"), SimpleNamespace(file_id="big", file_unique_id="u1", width=100, height=50)],
        video=None,
        animation=None,
        document=None,
        chat=SimpleNamespace(id=-1002),
        message_id=11,
    )
    content = build_saved_post_content_from_aiogram_message(message)
    assert content["kind"] == "photo"
    assert content["media"]["file_id"] == "big"
    assert content["caption"] == "Подпись"
    assert content["caption_entities"][0]["custom_emoji_id"] == "777"


def test_short_description_readable():
    assert get_saved_post_short_description({"kind": "photo"}) == "фото"


def test_build_content_text_has_empty_caption_fields():
    message = SimpleNamespace(
        text="hello",
        caption=None,
        entities=[SimpleNamespace(type="italic", offset=0, length=5)],
        caption_entities=None,
        photo=None,
        video=None,
        animation=None,
        document=None,
        chat=SimpleNamespace(id=-1003),
        message_id=12,
    )
    content = build_saved_post_content_from_aiogram_message(message)
    assert content["kind"] == "text"
    assert content["text"] == "hello"
    assert content["entities"]
    assert content["caption"] == ""


def test_summarize_message_and_entities_for_media():
    message = SimpleNamespace(
        message_id=123,
        content_type="photo",
        text=None,
        entities=None,
        caption="hello",
        caption_entities=[SimpleNamespace(type="custom_emoji", offset=0, length=1, custom_emoji_id="ce1")],
        photo=[SimpleNamespace(file_id="a"), SimpleNamespace(file_id="b", file_unique_id="u2", width=20, height=20)],
        video=None,
        animation=None,
        document=None,
        media_group_id="grp1",
        forward_origin=SimpleNamespace(),
    )
    summary = summarize_aiogram_message_for_saved_post(message)
    assert summary["has_caption"] is True
    assert summary["caption_len"] == 5
    assert summary["caption_entities_count"] == 1

    content = build_saved_post_content_from_aiogram_message(message)
    entity_summary = summarize_saved_post_entities(content)
    assert entity_summary["caption_entities_count"] == 1
    assert entity_summary["caption_entities_custom_emoji_count"] == 1


def test_get_saved_post_preview_caption():
    assert get_saved_post_preview_caption({"kind": "text", "text": "hello"}) == "hello"


def test_build_album_content():
    m1 = SimpleNamespace(message_id=1, media_group_id="g", photo=[SimpleNamespace(file_id="p1", file_unique_id="u1", width=1, height=1)], video=None, document=None, caption="", caption_entities=None, chat=SimpleNamespace(id=-100))
    m2 = SimpleNamespace(message_id=2, media_group_id="g", photo=[SimpleNamespace(file_id="p2", file_unique_id="u2", width=2, height=2)], video=None, document=None, caption="cap", caption_entities=[SimpleNamespace(type="bold", offset=0, length=3)], chat=SimpleNamespace(id=-100))
    m3 = SimpleNamespace(message_id=3, media_group_id="g", photo=None, video=SimpleNamespace(file_id="v", file_unique_id="u3", width=3, height=3, duration=5, mime_type="video/mp4", file_name=None), document=None, caption="", caption_entities=None, chat=SimpleNamespace(id=-100))
    from app.saved_posts_service import build_saved_post_album_content_from_aiogram_messages
    c = build_saved_post_album_content_from_aiogram_messages([m3, m1, m2])
    assert c["kind"] == "album"
    assert len(c["media_items"]) == 3
    assert c["caption"] == "cap"
    assert c["caption_entities"][0]["type"] == "bold"
    assert c["source_message_ids"] == [1, 2, 3]
    assert c["forward_origin"]["message_ids"] == [1, 2, 3]


def test_build_album_content_different_media_group_fails():
    from app.saved_posts_service import build_saved_post_album_content_from_aiogram_messages
    m1 = SimpleNamespace(message_id=1, media_group_id="g1", photo=[SimpleNamespace(file_id="p1")], video=None, document=None, caption="", caption_entities=None, chat=SimpleNamespace(id=-100))
    m2 = SimpleNamespace(message_id=2, media_group_id="g2", photo=[SimpleNamespace(file_id="p2")], video=None, document=None, caption="", caption_entities=None, chat=SimpleNamespace(id=-100))
    import pytest
    with pytest.raises(ValueError):
        build_saved_post_album_content_from_aiogram_messages([m1, m2])


def test_short_description_album():
    assert get_saved_post_short_description({"kind": "album", "media_items": [1, 2, 3]}) == "альбом · 3 медиа"
