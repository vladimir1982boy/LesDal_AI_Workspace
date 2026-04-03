from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from .domain import Channel, ContactIdentity, ConversationMode, ConversationSnapshot, ConversationStatus, InboundMessage, LeadStage, SenderRole


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return getattr(value, "value")
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


def _normalize_status(raw_status: str | None) -> ConversationStatus:
    status = str(raw_status or ConversationStatus.NEW.value).strip().lower()
    if status == "open":
        status = ConversationStatus.NEW.value
    return ConversationStatus(status)


def _parse_optional_datetime(raw_value: object) -> datetime | None:
    if not raw_value:
        return None
    return datetime.fromisoformat(str(raw_value))


class SQLiteLeadRepository:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        # On some synced/network Windows workspaces SQLite journal files can fail
        # with disk I/O errors, while memory journal mode remains stable.
        conn.execute("PRAGMA journal_mode=MEMORY;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _existing_columns(self, conn: sqlite3.Connection, table_name: str) -> set[str]:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(row["name"]) for row in rows}

    def _ensure_conversation_columns(self, conn: sqlite3.Connection) -> None:
        existing = self._existing_columns(conn, "conversations")
        required_columns = {
            "owner_name": "TEXT NOT NULL DEFAULT ''",
            "owner_claimed_at": "TEXT",
            "last_customer_message_at": "TEXT",
            "last_manager_message_at": "TEXT",
            "needs_attention": "INTEGER NOT NULL DEFAULT 0",
        }
        for column_name, column_sql in required_columns.items():
            if column_name in existing:
                continue
            conn.execute(
                f"ALTER TABLE conversations ADD COLUMN {column_name} {column_sql}"
            )

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS contacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel TEXT NOT NULL,
                    external_user_id TEXT NOT NULL,
                    username TEXT NOT NULL DEFAULT '',
                    display_name TEXT NOT NULL DEFAULT '',
                    phone TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(channel, external_user_id)
                );

                CREATE TABLE IF NOT EXISTS leads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contact_id INTEGER NOT NULL UNIQUE,
                    source_channel TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    city TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '',
                    interested_products TEXT NOT NULL DEFAULT '[]',
                    tags TEXT NOT NULL DEFAULT '[]',
                    amocrm_lead_id TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_message_at TEXT NOT NULL,
                    FOREIGN KEY(contact_id) REFERENCES contacts(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contact_id INTEGER NOT NULL,
                    channel TEXT NOT NULL,
                    external_chat_id TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'new',
                    owner_name TEXT NOT NULL DEFAULT '',
                    owner_claimed_at TEXT,
                    last_customer_message_at TEXT,
                    last_manager_message_at TEXT,
                    needs_attention INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_message_at TEXT NOT NULL,
                    UNIQUE(channel, external_chat_id),
                    FOREIGN KEY(contact_id) REFERENCES contacts(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id INTEGER NOT NULL,
                    sender_role TEXT NOT NULL,
                    sender_name TEXT NOT NULL DEFAULT '',
                    text TEXT NOT NULL,
                    raw_payload TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS lead_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(lead_id) REFERENCES leads(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS conversation_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    actor TEXT NOT NULL DEFAULT '',
                    payload TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS inbound_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel TEXT NOT NULL,
                    event_key TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(channel, event_key)
                );
                """
            )
            self._ensure_conversation_columns(conn)
            conn.execute(
                """
                UPDATE conversations
                SET status = ?
                WHERE status = 'open'
                """,
                (ConversationStatus.NEW.value,),
            )

    def ensure_contact(self, identity: ContactIdentity) -> int:
        now = _utcnow_iso()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id FROM contacts
                WHERE channel = ? AND external_user_id = ?
                """,
                (identity.channel.value, identity.external_user_id),
            ).fetchone()
            if row:
                conn.execute(
                    """
                    UPDATE contacts
                    SET username = ?, display_name = ?, phone = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        identity.username,
                        identity.display_name,
                        identity.phone,
                        now,
                        row["id"],
                    ),
                )
                return int(row["id"])

            cursor = conn.execute(
                """
                INSERT INTO contacts (
                    channel, external_user_id, username, display_name, phone, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    identity.channel.value,
                    identity.external_user_id,
                    identity.username,
                    identity.display_name,
                    identity.phone,
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def ensure_lead(
        self,
        *,
        contact_id: int,
        source_channel: Channel,
        stage: LeadStage = LeadStage.NEW,
        mode: ConversationMode = ConversationMode.AI,
    ) -> int:
        now = _utcnow_iso()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM leads WHERE contact_id = ?",
                (contact_id,),
            ).fetchone()
            if row:
                conn.execute(
                    """
                    UPDATE leads
                    SET updated_at = ?, last_message_at = ?
                    WHERE id = ?
                    """,
                    (now, now, row["id"]),
                )
                return int(row["id"])

            cursor = conn.execute(
                """
                INSERT INTO leads (
                    contact_id, source_channel, stage, mode, created_at, updated_at, last_message_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    contact_id,
                    source_channel.value,
                    stage.value,
                    mode.value,
                    now,
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def ensure_conversation(
        self,
        *,
        contact_id: int,
        channel: Channel,
        external_chat_id: str,
        mode: ConversationMode = ConversationMode.AI,
    ) -> int:
        now = _utcnow_iso()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id FROM conversations
                WHERE channel = ? AND external_chat_id = ?
                """,
                (channel.value, external_chat_id),
            ).fetchone()
            if row:
                conn.execute(
                    """
                    UPDATE conversations
                    SET updated_at = ?, last_message_at = ?
                    WHERE id = ?
                    """,
                    (now, now, row["id"]),
                )
                return int(row["id"])

            cursor = conn.execute(
                """
                INSERT INTO conversations (
                    contact_id, channel, external_chat_id, mode, status, owner_name,
                    owner_claimed_at, last_customer_message_at, last_manager_message_at,
                    needs_attention, created_at, updated_at, last_message_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    contact_id,
                    channel.value,
                    external_chat_id,
                    mode.value,
                    ConversationStatus.NEW.value,
                    "",
                    None,
                    now,
                    None,
                    0,
                    now,
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def add_message(
        self,
        *,
        conversation_id: int,
        sender_role: SenderRole,
        text: str,
        sender_name: str = "",
        raw_payload: dict | None = None,
    ) -> int:
        now = _utcnow_iso()
        payload = json.dumps(raw_payload or {}, ensure_ascii=False)
        conversation_updates = ["updated_at = ?", "last_message_at = ?"]
        conversation_params: list[object] = [now, now]
        if sender_role == SenderRole.CUSTOMER:
            conversation_updates.append("last_customer_message_at = ?")
            conversation_params.append(now)
        elif sender_role == SenderRole.MANAGER:
            conversation_updates.append("last_manager_message_at = ?")
            conversation_params.append(now)
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO messages (
                    conversation_id, sender_role, sender_name, text, raw_payload, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (conversation_id, sender_role.value, sender_name, text, payload, now),
            )
            conn.execute(
                f"""
                UPDATE conversations
                SET {', '.join(conversation_updates)}
                WHERE id = ?
                """,
                (*conversation_params, conversation_id),
            )
            conn.execute(
                """
                UPDATE leads
                SET updated_at = ?, last_message_at = ?
                WHERE contact_id = (SELECT contact_id FROM conversations WHERE id = ?)
                """,
                (now, now, conversation_id),
            )
            return int(cursor.lastrowid)

    def add_lead_event(self, *, lead_id: int, event_type: str, payload: dict | None = None) -> int:
        now = _utcnow_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO lead_events (lead_id, event_type, payload, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (lead_id, event_type, json.dumps(payload or {}, ensure_ascii=False), now),
            )
            return int(cursor.lastrowid)

    def add_conversation_event(
        self,
        *,
        conversation_id: int,
        event_type: str,
        actor: str = "",
        payload: dict | None = None,
    ) -> int:
        now = _utcnow_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO conversation_events (
                    conversation_id, event_type, actor, payload, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    conversation_id,
                    event_type,
                    actor,
                    json.dumps(payload or {}, ensure_ascii=False),
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def register_inbound_event(self, *, channel: Channel, event_key: str) -> bool:
        now = _utcnow_iso()
        with self._connect() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO inbound_events (channel, event_key, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (channel.value, event_key, now),
                )
            except sqlite3.IntegrityError:
                return False
            return True

    def update_lead(
        self,
        *,
        lead_id: int,
        stage: LeadStage | None = None,
        mode: ConversationMode | None = None,
        summary: str | None = None,
        city: str | None = None,
        interested_products: list[str] | None = None,
        tags: list[str] | None = None,
        amocrm_lead_id: str | None = None,
    ) -> None:
        updates: list[str] = []
        params: list[object] = []

        if stage is not None:
            updates.append("stage = ?")
            params.append(stage.value)
        if mode is not None:
            updates.append("mode = ?")
            params.append(mode.value)
        if summary is not None:
            updates.append("summary = ?")
            params.append(summary)
        if city is not None:
            updates.append("city = ?")
            params.append(city)
        if interested_products is not None:
            updates.append("interested_products = ?")
            params.append(json.dumps(interested_products, ensure_ascii=False))
        if tags is not None:
            updates.append("tags = ?")
            params.append(json.dumps(tags, ensure_ascii=False))
        if amocrm_lead_id is not None:
            updates.append("amocrm_lead_id = ?")
            params.append(amocrm_lead_id)

        if not updates:
            return

        updates.append("updated_at = ?")
        params.append(_utcnow_iso())
        params.append(lead_id)

        with self._connect() as conn:
            conn.execute(
                f"UPDATE leads SET {', '.join(updates)} WHERE id = ?",
                params,
            )

    def set_conversation_mode(self, *, conversation_id: int, mode: ConversationMode) -> None:
        self.update_conversation_state(
            conversation_id=conversation_id,
            mode=mode,
        )

    def update_conversation_state(
        self,
        *,
        conversation_id: int,
        mode: ConversationMode | None = None,
        status: ConversationStatus | None = None,
        owner_name: str | None = None,
        owner_claimed_at: datetime | None = None,
        clear_owner: bool = False,
        needs_attention: bool | None = None,
    ) -> None:
        now = _utcnow_iso()
        updates: list[str] = []
        params: list[object] = []

        if mode is not None:
            updates.append("mode = ?")
            params.append(mode.value)
        if status is not None:
            updates.append("status = ?")
            params.append(status.value)
        if clear_owner:
            updates.append("owner_name = ?")
            updates.append("owner_claimed_at = ?")
            params.extend(["", None])
        else:
            if owner_name is not None:
                updates.append("owner_name = ?")
                params.append(owner_name)
            if owner_claimed_at is not None:
                updates.append("owner_claimed_at = ?")
                params.append(owner_claimed_at.isoformat())
        if needs_attention is not None:
            updates.append("needs_attention = ?")
            params.append(1 if needs_attention else 0)

        if not updates:
            return

        updates.append("updated_at = ?")
        params.append(now)
        params.append(conversation_id)

        with self._connect() as conn:
            conn.execute(
                f"""
                UPDATE conversations
                SET {', '.join(updates)}
                WHERE id = ?
                """,
                params,
            )
            if mode is not None:
                conn.execute(
                    """
                    UPDATE leads
                    SET mode = ?, updated_at = ?
                    WHERE contact_id = (SELECT contact_id FROM conversations WHERE id = ?)
                    """,
                    (mode.value, now, conversation_id),
                )

    def ingest_customer_message(self, message: InboundMessage) -> ConversationSnapshot:
        contact_id = self.ensure_contact(
            ContactIdentity(
                channel=message.channel,
                external_user_id=message.external_user_id,
                username=message.username,
                display_name=message.display_name,
            )
        )
        lead_id = self.ensure_lead(contact_id=contact_id, source_channel=message.channel)
        conversation_id = self.ensure_conversation(
            contact_id=contact_id,
            channel=message.channel,
            external_chat_id=message.external_chat_id,
        )
        self.add_message(
            conversation_id=conversation_id,
            sender_role=SenderRole.CUSTOMER,
            sender_name=message.display_name or message.username,
            text=message.text,
            raw_payload=message.raw_payload,
        )
        self.add_lead_event(
            lead_id=lead_id,
            event_type="customer_message",
            payload=asdict(message),
        )
        return self.get_snapshot(conversation_id)

    def get_snapshot(self, conversation_id: int) -> ConversationSnapshot:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    c.contact_id,
                    l.id AS lead_id,
                    c.id AS conversation_id,
                    c.channel,
                    ct.external_user_id,
                    c.external_chat_id,
                    l.stage,
                    c.mode,
                    c.status,
                    c.owner_name,
                    c.owner_claimed_at,
                    c.last_customer_message_at,
                    c.last_manager_message_at,
                    c.needs_attention,
                    l.city,
                    l.summary,
                    l.tags,
                    l.interested_products,
                    ct.display_name,
                    ct.username,
                    c.created_at,
                    c.updated_at
                FROM conversations c
                JOIN contacts ct ON ct.id = c.contact_id
                JOIN leads l ON l.contact_id = c.contact_id
                WHERE c.id = ?
                """,
                (conversation_id,),
            ).fetchone()
            if row is None:
                raise LookupError(f"Conversation {conversation_id} not found")

        return ConversationSnapshot(
            contact_id=int(row["contact_id"]),
            lead_id=int(row["lead_id"]),
            conversation_id=int(row["conversation_id"]),
            channel=Channel(row["channel"]),
            external_user_id=str(row["external_user_id"]),
            external_chat_id=str(row["external_chat_id"]),
            stage=LeadStage(row["stage"]),
            mode=ConversationMode(row["mode"]),
            summary=str(row["summary"] or ""),
            display_name=str(row["display_name"] or ""),
            username=str(row["username"] or ""),
            city=str(row["city"] or ""),
            tags=json.loads(row["tags"] or "[]"),
            interested_products=json.loads(row["interested_products"] or "[]"),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            status=_normalize_status(row["status"]),
            owner_name=str(row["owner_name"] or ""),
            owner_claimed_at=_parse_optional_datetime(row["owner_claimed_at"]),
            last_customer_message_at=_parse_optional_datetime(row["last_customer_message_at"]),
            last_manager_message_at=_parse_optional_datetime(row["last_manager_message_at"]),
            needs_attention=bool(row["needs_attention"]),
        )

    def build_transcript(self, conversation_id: int, *, limit: int = 30) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT sender_role, sender_name, text, created_at
                FROM messages
                WHERE conversation_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (conversation_id, limit),
            ).fetchall()

    def list_recent_conversations(self, *, limit: int = 20) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT
                    c.id,
                    c.channel,
                    c.external_chat_id,
                    c.mode,
                    CASE WHEN c.status = 'open' THEN 'new' ELSE c.status END AS status,
                    c.owner_name,
                    c.owner_claimed_at,
                    c.last_customer_message_at,
                    c.last_manager_message_at,
                    c.needs_attention,
                    l.stage,
                    l.summary,
                    ct.display_name,
                    ct.username,
                    c.last_message_at
                FROM conversations c
                JOIN contacts ct ON ct.id = c.contact_id
                JOIN leads l ON l.contact_id = c.contact_id
                ORDER BY c.last_message_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def get_conversation_target(self, conversation_id: int) -> dict:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    c.id,
                    c.channel,
                    c.external_chat_id,
                    c.mode,
                    CASE WHEN c.status = 'open' THEN 'new' ELSE c.status END AS status,
                    ct.external_user_id,
                    ct.display_name,
                    ct.username
                FROM conversations c
                JOIN contacts ct ON ct.id = c.contact_id
                WHERE c.id = ?
                """,
                (conversation_id,),
            ).fetchone()
            if row is None:
                raise LookupError(f"Conversation {conversation_id} not found")
            return dict(row)

    def list_conversation_events(self, conversation_id: int, *, limit: int = 50) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT event_type, actor, payload, created_at
                FROM conversation_events
                WHERE conversation_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (conversation_id, limit),
            ).fetchall()


class JSONLeadRepository:
    def __init__(self, db_path: str | Path) -> None:
        self.path = Path(db_path).with_suffix(".json")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._save(
                {
                    "counters": {
                        "contacts": 0,
                        "leads": 0,
                        "conversations": 0,
                        "messages": 0,
                        "lead_events": 0,
                        "conversation_events": 0,
                    },
                    "contacts": [],
                    "leads": [],
                    "conversations": [],
                    "messages": [],
                    "lead_events": [],
                    "conversation_events": [],
                    "inbound_events": [],
                }
            )

    def _load(self) -> dict:
        data = json.loads(self.path.read_text(encoding="utf-8-sig"))
        counters = data.setdefault("counters", {})
        counters.setdefault("contacts", 0)
        counters.setdefault("leads", 0)
        counters.setdefault("conversations", 0)
        counters.setdefault("messages", 0)
        counters.setdefault("lead_events", 0)
        counters.setdefault("conversation_events", 0)
        data.setdefault("inbound_events", [])
        data.setdefault("conversation_events", [])
        for conversation in data.get("conversations", []):
            conversation.setdefault("status", ConversationStatus.NEW.value)
            if conversation["status"] == "open":
                conversation["status"] = ConversationStatus.NEW.value
            conversation.setdefault("owner_name", "")
            conversation.setdefault("owner_claimed_at", None)
            conversation.setdefault("last_customer_message_at", None)
            conversation.setdefault("last_manager_message_at", None)
            conversation.setdefault("needs_attention", False)
        return data

    def _save(self, data: dict) -> None:
        self.path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2, default=_json_default),
            encoding="utf-8",
        )

    def _next_id(self, data: dict, key: str) -> int:
        data["counters"][key] += 1
        return int(data["counters"][key])

    def ensure_contact(self, identity: ContactIdentity) -> int:
        data = self._load()
        now = _utcnow_iso()
        for contact in data["contacts"]:
            if (
                contact["channel"] == identity.channel.value
                and contact["external_user_id"] == identity.external_user_id
            ):
                contact["username"] = identity.username
                contact["display_name"] = identity.display_name
                contact["phone"] = identity.phone
                contact["updated_at"] = now
                self._save(data)
                return int(contact["id"])

        contact_id = self._next_id(data, "contacts")
        data["contacts"].append(
            {
                "id": contact_id,
                "channel": identity.channel.value,
                "external_user_id": identity.external_user_id,
                "username": identity.username,
                "display_name": identity.display_name,
                "phone": identity.phone,
                "created_at": now,
                "updated_at": now,
            }
        )
        self._save(data)
        return contact_id

    def ensure_lead(
        self,
        *,
        contact_id: int,
        source_channel: Channel,
        stage: LeadStage = LeadStage.NEW,
        mode: ConversationMode = ConversationMode.AI,
    ) -> int:
        data = self._load()
        now = _utcnow_iso()
        for lead in data["leads"]:
            if int(lead["contact_id"]) == contact_id:
                lead["updated_at"] = now
                lead["last_message_at"] = now
                self._save(data)
                return int(lead["id"])

        lead_id = self._next_id(data, "leads")
        data["leads"].append(
            {
                "id": lead_id,
                "contact_id": contact_id,
                "source_channel": source_channel.value,
                "stage": stage.value,
                "mode": mode.value,
                "city": "",
                "summary": "",
                "interested_products": [],
                "tags": [],
                "amocrm_lead_id": "",
                "created_at": now,
                "updated_at": now,
                "last_message_at": now,
            }
        )
        self._save(data)
        return lead_id

    def ensure_conversation(
        self,
        *,
        contact_id: int,
        channel: Channel,
        external_chat_id: str,
        mode: ConversationMode = ConversationMode.AI,
    ) -> int:
        data = self._load()
        now = _utcnow_iso()
        for conversation in data["conversations"]:
            if (
                conversation["channel"] == channel.value
                and conversation["external_chat_id"] == external_chat_id
            ):
                conversation["updated_at"] = now
                conversation["last_message_at"] = now
                self._save(data)
                return int(conversation["id"])

        conversation_id = self._next_id(data, "conversations")
        data["conversations"].append(
            {
                "id": conversation_id,
                "contact_id": contact_id,
                "channel": channel.value,
                "external_chat_id": external_chat_id,
                "mode": mode.value,
                "status": ConversationStatus.NEW.value,
                "owner_name": "",
                "owner_claimed_at": None,
                "last_customer_message_at": now,
                "last_manager_message_at": None,
                "needs_attention": False,
                "created_at": now,
                "updated_at": now,
                "last_message_at": now,
            }
        )
        self._save(data)
        return conversation_id

    def add_message(
        self,
        *,
        conversation_id: int,
        sender_role: SenderRole,
        text: str,
        sender_name: str = "",
        raw_payload: dict | None = None,
    ) -> int:
        data = self._load()
        now = _utcnow_iso()
        message_id = self._next_id(data, "messages")
        data["messages"].append(
            {
                "id": message_id,
                "conversation_id": conversation_id,
                "sender_role": sender_role.value,
                "sender_name": sender_name,
                "text": text,
                "raw_payload": raw_payload or {},
                "created_at": now,
            }
        )
        for conversation in data["conversations"]:
            if int(conversation["id"]) == conversation_id:
                conversation["updated_at"] = now
                conversation["last_message_at"] = now
                if sender_role == SenderRole.CUSTOMER:
                    conversation["last_customer_message_at"] = now
                elif sender_role == SenderRole.MANAGER:
                    conversation["last_manager_message_at"] = now
                contact_id = int(conversation["contact_id"])
                for lead in data["leads"]:
                    if int(lead["contact_id"]) == contact_id:
                        lead["updated_at"] = now
                        lead["last_message_at"] = now
                        break
                break
        self._save(data)
        return message_id

    def add_lead_event(self, *, lead_id: int, event_type: str, payload: dict | None = None) -> int:
        data = self._load()
        event_id = self._next_id(data, "lead_events")
        data["lead_events"].append(
            {
                "id": event_id,
                "lead_id": lead_id,
                "event_type": event_type,
                "payload": payload or {},
                "created_at": _utcnow_iso(),
            }
        )
        self._save(data)
        return event_id

    def add_conversation_event(
        self,
        *,
        conversation_id: int,
        event_type: str,
        actor: str = "",
        payload: dict | None = None,
    ) -> int:
        data = self._load()
        event_id = self._next_id(data, "conversation_events")
        data["conversation_events"].append(
            {
                "id": event_id,
                "conversation_id": conversation_id,
                "event_type": event_type,
                "actor": actor,
                "payload": payload or {},
                "created_at": _utcnow_iso(),
            }
        )
        self._save(data)
        return event_id

    def register_inbound_event(self, *, channel: Channel, event_key: str) -> bool:
        data = self._load()
        for row in data["inbound_events"]:
            if row["channel"] == channel.value and row["event_key"] == event_key:
                return False

        data["inbound_events"].append(
            {
                "channel": channel.value,
                "event_key": event_key,
                "created_at": _utcnow_iso(),
            }
        )
        self._save(data)
        return True

    def update_lead(
        self,
        *,
        lead_id: int,
        stage: LeadStage | None = None,
        mode: ConversationMode | None = None,
        summary: str | None = None,
        city: str | None = None,
        interested_products: list[str] | None = None,
        tags: list[str] | None = None,
        amocrm_lead_id: str | None = None,
    ) -> None:
        data = self._load()
        for lead in data["leads"]:
            if int(lead["id"]) != lead_id:
                continue
            if stage is not None:
                lead["stage"] = stage.value
            if mode is not None:
                lead["mode"] = mode.value
            if summary is not None:
                lead["summary"] = summary
            if city is not None:
                lead["city"] = city
            if interested_products is not None:
                lead["interested_products"] = interested_products
            if tags is not None:
                lead["tags"] = tags
            if amocrm_lead_id is not None:
                lead["amocrm_lead_id"] = amocrm_lead_id
            lead["updated_at"] = _utcnow_iso()
            break
        self._save(data)

    def set_conversation_mode(self, *, conversation_id: int, mode: ConversationMode) -> None:
        self.update_conversation_state(
            conversation_id=conversation_id,
            mode=mode,
        )

    def update_conversation_state(
        self,
        *,
        conversation_id: int,
        mode: ConversationMode | None = None,
        status: ConversationStatus | None = None,
        owner_name: str | None = None,
        owner_claimed_at: datetime | None = None,
        clear_owner: bool = False,
        needs_attention: bool | None = None,
    ) -> None:
        data = self._load()
        now = _utcnow_iso()
        contact_id = None
        for conversation in data["conversations"]:
            if int(conversation["id"]) != conversation_id:
                continue
            if mode is not None:
                conversation["mode"] = mode.value
            if status is not None:
                conversation["status"] = status.value
            if clear_owner:
                conversation["owner_name"] = ""
                conversation["owner_claimed_at"] = None
            else:
                if owner_name is not None:
                    conversation["owner_name"] = owner_name
                if owner_claimed_at is not None:
                    conversation["owner_claimed_at"] = owner_claimed_at.isoformat()
            if needs_attention is not None:
                conversation["needs_attention"] = bool(needs_attention)
            conversation["updated_at"] = now
            contact_id = int(conversation["contact_id"])
            break
        if contact_id is not None and mode is not None:
            for lead in data["leads"]:
                if int(lead["contact_id"]) == contact_id:
                    lead["mode"] = mode.value
                    lead["updated_at"] = now
                    break
        self._save(data)

    def ingest_customer_message(self, message: InboundMessage) -> ConversationSnapshot:
        contact_id = self.ensure_contact(
            ContactIdentity(
                channel=message.channel,
                external_user_id=message.external_user_id,
                username=message.username,
                display_name=message.display_name,
            )
        )
        lead_id = self.ensure_lead(contact_id=contact_id, source_channel=message.channel)
        conversation_id = self.ensure_conversation(
            contact_id=contact_id,
            channel=message.channel,
            external_chat_id=message.external_chat_id,
        )
        self.add_message(
            conversation_id=conversation_id,
            sender_role=SenderRole.CUSTOMER,
            sender_name=message.display_name or message.username,
            text=message.text,
            raw_payload=message.raw_payload,
        )
        self.add_lead_event(
            lead_id=lead_id,
            event_type="customer_message",
            payload=asdict(message),
        )
        return self.get_snapshot(conversation_id)

    def get_snapshot(self, conversation_id: int) -> ConversationSnapshot:
        data = self._load()
        conversation = next(
            item for item in data["conversations"] if int(item["id"]) == conversation_id
        )
        contact = next(
            (
                item
                for item in data["contacts"]
                if int(item["id"]) == int(conversation["contact_id"])
            ),
            {},
        )
        lead = next(
            item for item in data["leads"] if int(item["contact_id"]) == int(conversation["contact_id"])
        )
        return ConversationSnapshot(
            contact_id=int(conversation["contact_id"]),
            lead_id=int(lead["id"]),
            conversation_id=int(conversation["id"]),
            channel=Channel(conversation["channel"]),
            external_user_id=str(contact.get("external_user_id", "")),
            external_chat_id=str(conversation["external_chat_id"]),
            stage=LeadStage(lead["stage"]),
            mode=ConversationMode(conversation["mode"]),
            summary=str(lead.get("summary", "")),
            display_name=str(contact.get("display_name", "")),
            username=str(contact.get("username", "")),
            city=str(lead.get("city", "")),
            tags=list(lead.get("tags", [])),
            interested_products=list(lead.get("interested_products", [])),
            created_at=datetime.fromisoformat(conversation["created_at"]),
            updated_at=datetime.fromisoformat(conversation["updated_at"]),
            status=_normalize_status(conversation.get("status")),
            owner_name=str(conversation.get("owner_name", "")),
            owner_claimed_at=_parse_optional_datetime(conversation.get("owner_claimed_at")),
            last_customer_message_at=_parse_optional_datetime(conversation.get("last_customer_message_at")),
            last_manager_message_at=_parse_optional_datetime(conversation.get("last_manager_message_at")),
            needs_attention=bool(conversation.get("needs_attention", False)),
        )

    def build_transcript(self, conversation_id: int, *, limit: int = 30) -> list[dict]:
        data = self._load()
        rows = [
            row for row in data["messages"] if int(row["conversation_id"]) == conversation_id
        ]
        rows.sort(key=lambda item: int(item["id"]), reverse=True)
        return rows[:limit]

    def list_recent_conversations(self, *, limit: int = 20) -> list[dict]:
        data = self._load()
        contacts = {int(contact["id"]): contact for contact in data["contacts"]}
        leads = {int(lead["contact_id"]): lead for lead in data["leads"]}
        rows: list[dict] = []
        for conversation in data["conversations"]:
            contact_id = int(conversation["contact_id"])
            contact = contacts.get(contact_id, {})
            lead = leads.get(contact_id, {})
            rows.append(
                {
                    "id": conversation["id"],
                    "channel": conversation["channel"],
                    "external_chat_id": conversation["external_chat_id"],
                    "mode": conversation["mode"],
                    "status": _normalize_status(conversation.get("status")).value,
                    "owner_name": conversation.get("owner_name", ""),
                    "owner_claimed_at": conversation.get("owner_claimed_at"),
                    "last_customer_message_at": conversation.get("last_customer_message_at"),
                    "last_manager_message_at": conversation.get("last_manager_message_at"),
                    "needs_attention": bool(conversation.get("needs_attention", False)),
                    "stage": lead.get("stage", LeadStage.NEW.value),
                    "summary": lead.get("summary", ""),
                    "display_name": contact.get("display_name", ""),
                    "username": contact.get("username", ""),
                    "last_message_at": conversation["last_message_at"],
                }
            )
        rows.sort(key=lambda item: item["last_message_at"], reverse=True)
        return rows[:limit]

    def get_conversation_target(self, conversation_id: int) -> dict:
        data = self._load()
        conversation = next(
            item for item in data["conversations"] if int(item["id"]) == conversation_id
        )
        contact = next(
            (
                item
                for item in data["contacts"]
                if int(item["id"]) == int(conversation["contact_id"])
            ),
            {},
        )
        return {
            "id": int(conversation["id"]),
            "channel": conversation["channel"],
            "external_chat_id": conversation["external_chat_id"],
            "mode": conversation["mode"],
            "status": _normalize_status(conversation.get("status")).value,
            "external_user_id": contact.get("external_user_id", ""),
            "display_name": contact.get("display_name", ""),
            "username": contact.get("username", ""),
        }

    def list_conversation_events(self, conversation_id: int, *, limit: int = 50) -> list[dict]:
        data = self._load()
        rows = [
            row
            for row in data["conversation_events"]
            if int(row["conversation_id"]) == conversation_id
        ]
        rows.sort(key=lambda item: int(item["id"]), reverse=True)
        return rows[:limit]
