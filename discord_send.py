from __future__ import annotations

import os

import requests


DISCORD_LIMIT = 2000


def chunk_message(message: str, limit: int = DISCORD_LIMIT) -> list[str]:
    if len(message) <= limit:
        return [message]
    chunks = []
    remaining = message
    while remaining:
        chunk = remaining[:limit]
        split_at = chunk.rfind("\n")
        if split_at > limit * 0.5:
            chunk = chunk[:split_at]
        chunks.append(chunk)
        remaining = remaining[len(chunk) :].lstrip("\n")
    return chunks


def discord_send(message: str, webhook_url: str | None = None) -> None:
    webhook_url = webhook_url or os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        raise RuntimeError("No Discord webhook URL provided")
    for chunk in chunk_message(message):
        response = requests.post(webhook_url, json={"content": chunk}, timeout=30)
        response.raise_for_status()
