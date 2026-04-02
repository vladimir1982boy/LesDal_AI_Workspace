from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from .config import SalesBotConfig
from .domain import ConversationSnapshot


logger = logging.getLogger("lesdal.ai_sales.google_sheets")
SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
LEAD_COLUMNS = [
    "conversation_id",
    "lead_id",
    "contact_id",
    "created_at",
    "updated_at",
    "channel",
    "external_user_id",
    "external_chat_id",
    "stage",
    "mode",
    "display_name",
    "username",
    "city",
    "interested_products",
    "tags",
    "summary",
    "last_sender",
    "last_message",
    "manager_summary",
]


def _column_letter(index: int) -> str:
    result = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


@dataclass(slots=True)
class GoogleSheetsLeadSync:
    config: SalesBotConfig

    @classmethod
    def from_config(cls, config: SalesBotConfig) -> "GoogleSheetsLeadSync | None":
        if not config.has_google_sheets:
            return None
        return cls(config)

    @property
    def sheet_name(self) -> str:
        return self.config.google_sheets_leads_sheet

    def sync_lead(
        self,
        *,
        snapshot: ConversationSnapshot,
        last_sender: str,
        last_message: str,
        manager_summary: str,
    ) -> bool:
        try:
            service = self._build_service()
            self._ensure_headers(service)
            row_values = self._build_row(
                snapshot=snapshot,
                last_sender=last_sender,
                last_message=last_message,
                manager_summary=manager_summary,
            )
            existing_row = self._find_existing_row(service, snapshot.conversation_id)
            end_col = _column_letter(len(LEAD_COLUMNS))
            if existing_row is not None:
                service.spreadsheets().values().update(
                    spreadsheetId=self.config.google_sheets_spreadsheet_id,
                    range=f"{self.sheet_name}!A{existing_row}:{end_col}{existing_row}",
                    valueInputOption="RAW",
                    body={"values": [row_values]},
                ).execute()
            else:
                service.spreadsheets().values().append(
                    spreadsheetId=self.config.google_sheets_spreadsheet_id,
                    range=f"{self.sheet_name}!A:{end_col}",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": [row_values]},
                ).execute()
            return True
        except Exception as exc:
            logger.warning("Google Sheets sync failed: %s", exc)
            return False

    def _build_service(self):
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        credentials = None
        if self.config.google_sheets_credentials_json:
            info = json.loads(self.config.google_sheets_credentials_json)
            credentials = service_account.Credentials.from_service_account_info(
                info,
                scopes=[SHEETS_SCOPE],
            )
        elif self.config.google_sheets_credentials_path and self.config.google_sheets_credentials_path.is_file():
            credentials = service_account.Credentials.from_service_account_file(
                str(self.config.google_sheets_credentials_path),
                scopes=[SHEETS_SCOPE],
            )
        else:
            raise RuntimeError("Google Sheets credentials are not configured")

        return build("sheets", "v4", credentials=credentials, cache_discovery=False)

    def _ensure_headers(self, service) -> None:
        self._ensure_sheet_exists(service)
        response = service.spreadsheets().values().get(
            spreadsheetId=self.config.google_sheets_spreadsheet_id,
            range=f"{self.sheet_name}!1:1",
        ).execute()
        values = response.get("values", [])
        if values and values[0] == LEAD_COLUMNS:
            return

        end_col = _column_letter(len(LEAD_COLUMNS))
        service.spreadsheets().values().update(
            spreadsheetId=self.config.google_sheets_spreadsheet_id,
            range=f"{self.sheet_name}!A1:{end_col}1",
            valueInputOption="RAW",
            body={"values": [LEAD_COLUMNS]},
        ).execute()

    def _ensure_sheet_exists(self, service) -> None:
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=self.config.google_sheets_spreadsheet_id,
        ).execute()
        sheets = spreadsheet.get("sheets", [])
        for sheet in sheets:
            props = sheet.get("properties", {})
            if props.get("title") == self.sheet_name:
                return

        service.spreadsheets().batchUpdate(
            spreadsheetId=self.config.google_sheets_spreadsheet_id,
            body={
                "requests": [
                    {
                        "addSheet": {
                            "properties": {
                                "title": self.sheet_name,
                            }
                        }
                    }
                ]
            },
        ).execute()

    def _find_existing_row(self, service, conversation_id: int) -> int | None:
        response = service.spreadsheets().values().get(
            spreadsheetId=self.config.google_sheets_spreadsheet_id,
            range=f"{self.sheet_name}!A2:A",
        ).execute()
        values = response.get("values", [])
        needle = str(conversation_id)
        for index, row in enumerate(values, start=2):
            if row and str(row[0]).strip() == needle:
                return index
        return None

    def _build_row(
        self,
        *,
        snapshot: ConversationSnapshot,
        last_sender: str,
        last_message: str,
        manager_summary: str,
    ) -> list[str]:
        return [
            str(snapshot.conversation_id),
            str(snapshot.lead_id),
            str(snapshot.contact_id),
            snapshot.created_at.isoformat(),
            snapshot.updated_at.isoformat(),
            snapshot.channel.value,
            snapshot.external_user_id,
            snapshot.external_chat_id,
            snapshot.stage.value,
            snapshot.mode.value,
            snapshot.display_name,
            snapshot.username,
            snapshot.city,
            ", ".join(snapshot.interested_products),
            ", ".join(snapshot.tags),
            snapshot.summary,
            last_sender,
            last_message,
            manager_summary,
        ]
