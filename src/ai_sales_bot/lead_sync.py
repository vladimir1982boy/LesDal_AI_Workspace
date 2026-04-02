from __future__ import annotations

from dataclasses import dataclass

from .config import SalesBotConfig
from .domain import ConversationSnapshot
from .google_sheets import GoogleSheetsLeadSync
from .services import SalesBotService


@dataclass(slots=True)
class LeadSyncCoordinator:
    service: SalesBotService
    google_sheets: GoogleSheetsLeadSync | None = None

    @classmethod
    def from_config(
        cls,
        *,
        config: SalesBotConfig,
        service: SalesBotService,
    ) -> "LeadSyncCoordinator":
        return cls(
            service=service,
            google_sheets=GoogleSheetsLeadSync.from_config(config),
        )

    def sync_conversation(self, conversation_id: int) -> bool:
        snapshot = self.service.get_snapshot(conversation_id)
        return self.sync_snapshot(snapshot)

    def sync_snapshot(self, snapshot: ConversationSnapshot) -> bool:
        if self.google_sheets is None:
            return False

        transcript = self.service.get_transcript(
            conversation_id=snapshot.conversation_id,
            limit=1,
        )
        last_sender = ""
        last_message = ""
        if transcript:
            row = transcript[0]
            last_sender = str(row.get("sender_name") or row.get("sender_role") or "")
            last_message = str(row.get("text") or "")

        manager_summary = self.service.build_manager_summary(
            conversation_id=snapshot.conversation_id,
            limit=8,
        )
        return self.google_sheets.sync_lead(
            snapshot=snapshot,
            last_sender=last_sender,
            last_message=last_message,
            manager_summary=manager_summary,
        )
