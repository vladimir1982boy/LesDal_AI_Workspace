from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass

from .catalog import ProductCatalog
from .config import SalesBotConfig
from .services import SalesBotService
from .storage import JSONLeadRepository, SQLiteLeadRepository


logger = logging.getLogger("lesdal.ai_sales")


@dataclass(slots=True)
class SalesBotRuntime:
    config: SalesBotConfig
    repository: SQLiteLeadRepository | JSONLeadRepository
    service: SalesBotService
    catalog: ProductCatalog


def create_runtime(config: SalesBotConfig | None = None) -> SalesBotRuntime:
    cfg = config or SalesBotConfig.from_env()
    try:
        repository = SQLiteLeadRepository(cfg.db_path)
    except sqlite3.OperationalError:
        fallback_path = cfg.db_path.with_suffix(".json")
        logger.warning(
            "SQLite storage is unavailable in this workspace. Falling back to JSON storage at %s",
            fallback_path,
        )
        repository = JSONLeadRepository(fallback_path)
    service = SalesBotService(repository)
    catalog = ProductCatalog.from_json(cfg.catalog_path)
    return SalesBotRuntime(
        config=cfg,
        repository=repository,
        service=service,
        catalog=catalog,
    )
