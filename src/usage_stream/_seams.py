"""Private synchronization points at the fetch and commit boundaries. Tests install hooks."""
from collections.abc import Callable
from typing import Literal

Stage = Literal["fetched", "before_commit", "after_commit"]
Hook = Callable[[str, int], None]
_hook: Hook | None = None


def install(hook: Hook | None) -> None:
    global _hook
    _hook = hook


def reached(stage: Stage, offset: int) -> None:
    if _hook is not None:
        _hook(stage, offset)
