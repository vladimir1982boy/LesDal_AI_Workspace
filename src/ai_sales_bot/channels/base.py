from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from ..domain import Channel


@dataclass(slots=True)
class OutboundMessage:
    channel: Channel
    chat_id: str
    text: str
    attachments: list[str] = field(default_factory=list)


class ChannelAdapter(ABC):
    channel: Channel

    @abstractmethod
    def send(self, message: OutboundMessage) -> bool:
        raise NotImplementedError
