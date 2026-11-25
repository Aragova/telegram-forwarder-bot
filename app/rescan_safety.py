"""Expected operational rescan refusal."""


class RescanBlocked(RuntimeError):
    """A new publication cycle cannot safely begin."""
