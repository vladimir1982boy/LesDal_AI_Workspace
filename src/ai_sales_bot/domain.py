from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Channel(StrEnum):
    TELEGRAM = "telegram"
    MAX = "max"
    VK = "vk"
    WEB = "web"


class SenderRole(StrEnum):
    CUSTOMER = "customer"
    AI = "ai"
    MANAGER = "manager"
    SYSTEM = "system"


class ConversationMode(StrEnum):
    AI = "ai"
    MANAGER = "manager"
    HYBRID = "hybrid"


class ConversationStatus(StrEnum):
    NEW = "new"
    IN_PROGRESS = "in_progress"
    WAITING_MANAGER = "waiting_manager"
    CLOSED = "closed"


class LeadStage(StrEnum):
    NEW = "new"
    QUALIFIED = "qualified"
    PRODUCT_SELECTION = "product_selection"
    WAITING_CONTACTS = "waiting_contacts"
    CONTACTED = "contacted"
    PAYMENT_PENDING = "payment_pending"
    WON = "won"
    LOST = "lost"
    NURTURE = "nurture"


class LeadPriority(StrEnum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


@dataclass(slots=True)
class ContactIdentity:
    channel: Channel
    external_user_id: str
    username: str = ""
    display_name: str = ""
    phone: str = ""


@dataclass(slots=True)
class InboundMessage:
    channel: Channel
    external_user_id: str
    external_chat_id: str
    text: str
    username: str = ""
    display_name: str = ""
    raw_payload: dict | None = None


@dataclass(slots=True)
class ConversationSnapshot:
    contact_id: int
    lead_id: int
    conversation_id: int
    channel: Channel
    external_user_id: str
    external_chat_id: str
    stage: LeadStage
    mode: ConversationMode
    summary: str
    display_name: str = ""
    username: str = ""
    city: str = ""
    tags: list[str] = field(default_factory=list)
    interested_products: list[str] = field(default_factory=list)
    manager_notes: str = ""
    priority: LeadPriority = LeadPriority.NORMAL
    follow_up_date: str = ""
    next_action: str = ""
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)
    status: ConversationStatus = ConversationStatus.NEW
    owner_name: str = ""
    owner_claimed_at: datetime | None = None
    last_customer_message_at: datetime | None = None
    last_manager_message_at: datetime | None = None
    needs_attention: bool = False
