"""Core package for the LesDal AI sales assistant."""

from .app import SalesBotRuntime, create_runtime
from .conversation_flow import CustomerTurnResult, SalesConversationManager
from .config import SalesBotConfig
from .google_sheets import GoogleSheetsLeadSync
from .lead_sync import LeadSyncCoordinator
from .max_app import MaxSalesBot
from .operator_api import OperatorInboxAPI
from .outbound import OutboundDispatcher
from .services import SalesBotService
from .storage import JSONLeadRepository, SQLiteLeadRepository
from .vk_app import VKSalesBot

__all__ = [
    "SalesBotConfig",
    "CustomerTurnResult",
    "GoogleSheetsLeadSync",
    "JSONLeadRepository",
    "LeadSyncCoordinator",
    "MaxSalesBot",
    "OperatorInboxAPI",
    "OutboundDispatcher",
    "SalesConversationManager",
    "SalesBotRuntime",
    "SalesBotService",
    "SQLiteLeadRepository",
    "VKSalesBot",
    "create_runtime",
]
