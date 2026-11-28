"""Expected operational rescan refusal."""


class RescanBlocked(RuntimeError):
    """A new publication cycle cannot safely begin."""


class RescanSourceWriter:
    """Pass the durable owner into the existing parser's batch writes."""

    def __init__(self, repo, rule_id: int, owner_token: str):
        self.repo = repo
        self.owner = (int(rule_id), owner_token)

    def save_post_batch(self, posts_data, *, preserve_existing_deliveries=False):
        if not preserve_existing_deliveries:
            raise RescanBlocked("Пересканирование не должно заменять общие записи")
        return self.repo.save_post_batch(
            posts_data, preserve_existing_deliveries=True, rescan_owner=self.owner,
        )
