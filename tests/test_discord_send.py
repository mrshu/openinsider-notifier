from __future__ import annotations

from discord_send import chunk_message


def test_chunk_message_keeps_short_messages_whole() -> None:
    assert chunk_message("hello") == ["hello"]


def test_chunk_message_splits_long_messages() -> None:
    message = "a" * 2100

    chunks = chunk_message(message)

    assert len(chunks) == 2
    assert all(len(chunk) <= 2000 for chunk in chunks)
    assert "".join(chunks) == message
