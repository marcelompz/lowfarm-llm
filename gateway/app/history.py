"""Server-side conversation history.

The gateway keeps the authoritative transcript of a conversation in Redis so
that:

  * a client can resume a conversation without resending it;
  * the history can be trimmed to fit a worker's limited KV cache;
  * the transcript survives a gateway restart.

The contract with the client is deliberately forgiving — three shapes are
accepted, and the merge rules live in ``store.merge_history``:

| Client sends | Interpretation |
|---|---|
| full history + new turn | the client extends the conversation |
| a truncated history | the server keeps everything it has |
| a single new user message | "continue the conversation" |
| anything else that diverges | the client edited or branched — it wins |

Without a ``metadata.conversation_id`` the gateway stays completely
stateless: whatever the client sent is what the worker receives.
"""

from __future__ import annotations

from typing import Any

from .config import Settings
from .store import Store


class ChatHistory:
    def __init__(self, store: Store, settings: Settings) -> None:
        self._store = store
        self._settings = settings

    @property
    def enabled(self) -> bool:
        return self._settings.chat_history_enabled

    async def resolve(
        self, conversation_id: str | None, incoming: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Return the effective message list for this request.

        Also persists the merged history when a conversation id is present.
        """
        if not conversation_id or not self.enabled:
            return list(incoming)

        merged = await self._store.merge_chat(
            conversation_id, incoming, self._settings.chat_history_max_messages
        )
        return merged

    async def get(self, conversation_id: str) -> list[dict[str, Any]]:
        return await self._store.get_chat(conversation_id)

    async def record_assistant(
        self, conversation_id: str | None, content: str
    ) -> None:
        """Append the assistant's reply to the stored conversation.

        Without this the history would only ever contain what the client
        sent, and "continue the conversation" could not work: the merge rule
        needs the stored transcript to end with an assistant turn.
        """
        if not conversation_id or not self.enabled or not content:
            return
        await self._store.append_chat(
            conversation_id,
            [{"role": "assistant", "content": content}],
            self._settings.chat_history_max_messages,
        )

    async def delete(self, conversation_id: str) -> bool:
        return await self._store.delete_chat(conversation_id)
