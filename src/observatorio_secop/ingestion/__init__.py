"""Incremental Bronze ingestion for SECOP II contracts."""

from observatorio_secop.ingestion.config import IngestionConfig, RunWindow
from observatorio_secop.ingestion.contract import BronzeSourceContract

__all__ = ["BronzeSourceContract", "IngestionConfig", "RunWindow"]
