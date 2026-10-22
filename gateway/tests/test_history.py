"""Server-side conversation history."""

from __future__ import annotations

import pytest

from app.config import Settings
from app.history import ChatHistory
from app.store import MemoryStore, merge_history


def _msg(role: str, content: str) -> dict[str, str]:
    return {"role": role, "content": content}


@pytest.mark.parametrize(
    "stored,incoming,expected",
    [
        # Client sends the full history plus a new turn -> client wins.
        (
            [_msg("user", "a"), _msg("assistant", "b")],
            [_msg("user", "a"), _msg("assistant", "b"), _msg("user", "c")],
            [_msg("user", "a"), _msg("assistant", "b"), _msg("user", "c")],
        ),
        # Client sends a truncated history -> the server keeps everything.
        (
            [_msg("user", "a"), _msg("assistant", "b")],
            [_msg("user", "a")],
            [_msg("user", "a"), _msg("assistant", "b")],
        ),
        # A single new user message continues the conversation.
        (
            [_msg("user", "a"), _msg("assistant", "b")],
            [_msg("user", "c")],
            [_msg("user", "a"), _msg("assistant", "b"), _msg("user", "c")],
        ),
        # Divergence (the client edited the transcript) -> the client wins.
        (
            [_msg("user", "a"), _msg("assistant", "b")],
            [_msg("user", "a"), _msg("assistant", "EDITED")],
            [_msg("user", "a"), _msg("assistant", "EDITED")],
        ),
        # Empty stored history -> the client defines it.
        ([], [_msg("user", "a")], [_msg("user", "a")]),
        # Empty incoming -> keep what we have.
        ([_msg("user", "a")], [], [_msg("user", "a")]),
    ],
)
def test_merge_history(stored, incoming, expected) -> None:
    assert merge_history(stored, incoming) == expected


@pytest.mark.asyncio
async def test_history_accumulates_across_requests(
    store: MemoryStore, settings: Settings
) -> None:
    history = ChatHistory(store, settings)
    first = await history.resolve("conv-1", [_msg("user", "hola")])
    assert first == [_msg("user", "hola")]

    second = await history.resolve("conv-1", [_msg("user", "hola"), _msg("assistant", "hey")])
    assert len(second) == 2

    third = await history.resolve("conv-1", [_msg("user", "hola"), _msg("assistant", "hey"), _msg("user", "mas")])
    assert len(third) == 3


@pytest.mark.asyncio
async def test_history_is_isolated_per_conversation(
    store: MemoryStore, settings: Settings
) -> None:
    history = ChatHistory(store, settings)
    await history.resolve("conv-a", [_msg("user", "a")])
    other = await history.resolve("conv-b", [_msg("user", "b")])
    assert other == [_msg("user", "b")]


@pytest.mark.asyncio
async def test_history_is_trimmed_to_the_configured_window(
    store: MemoryStore, settings: Settings
) -> None:
    settings.chat_history_max_messages = 4
    history = ChatHistory(store, settings)
    messages: list[dict[str, str]] = []
    for i in range(10):
        messages = await history.resolve("conv-1", messages + [_msg("user", f"m{i}")])
    assert len(messages) == 4
    # The oldest turns are dropped, the newest are kept.
    assert messages[-1]["content"] == "m9"


@pytest.mark.asyncio
async def test_trimming_keeps_the_system_message(
    store: MemoryStore, settings: Settings
) -> None:
    settings.chat_history_max_messages = 3
    history = ChatHistory(store, settings)
    messages: list[dict[str, str]] = [_msg("system", "eres un asistente")]
    for i in range(6):
        messages = await history.resolve(
            "conv-sys", messages + [_msg("user", f"m{i}")]
        )
    assert len(messages) == 3
    assert messages[0]["role"] == "system"
    assert messages[0]["content"] == "eres un asistente"
    assert messages[-1]["content"] == "m5"


@pytest.mark.asyncio
async def test_delete_removes_the_conversation(
    store: MemoryStore, settings: Settings
) -> None:
    history = ChatHistory(store, settings)
    await history.resolve("conv-x", [_msg("user", "a")])
    assert await history.get("conv-x")
    assert await history.delete("conv-x") is True
    assert await history.get("conv-x") == []
    assert await history.delete("conv-x") is False


@pytest.mark.asyncio
async def test_history_is_optional(store: MemoryStore, settings: Settings) -> None:
    settings.chat_history_enabled = False
    history = ChatHistory(store, settings)
    result = await history.resolve("conv-1", [_msg("user", "a")])
    assert result == [_msg("user", "a")]
    # Nothing was persisted.
    assert await history.get("conv-1") == []
