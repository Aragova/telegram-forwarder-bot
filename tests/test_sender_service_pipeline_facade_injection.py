from app.sender import SenderService


class DummyBot:
    pass


class DummyRepo:
    pass


def test_sender_service_can_be_created_without_sender_pipeline_facade():
    service = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=DummyRepo())

    assert service.sender_pipeline_facade is None


def test_sender_service_stores_injected_sender_pipeline_facade():
    facade = object()

    service = SenderService(
        bot=DummyBot(),
        telethon_client=None,
        reaction_clients=[],
        db=DummyRepo(),
        sender_pipeline_facade=facade,
    )

    assert service.sender_pipeline_facade is facade
