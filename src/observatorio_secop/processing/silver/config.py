"""Validated configuration and territorial catalog for Silver processing."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from observatorio_secop.processing.silver.errors import SilverConfigurationError


@dataclass(frozen=True)
class TerritoryCatalog:
    version: str
    department: str
    provenance: str
    municipalities: tuple[str, ...]
    aliases: dict[str, str]
    valle_de_aburra: frozenset[str]
    digest: str

    @property
    def canonical_by_key(self) -> dict[str, str]:
        result = {territory_key(name): name for name in self.municipalities}
        result.update(
            {territory_key(alias): canonical for alias, canonical in self.aliases.items()}
        )
        return result


@dataclass(frozen=True)
class SilverConfig:
    dataset_id: str
    bronze_root: Path
    silver_root: Path
    contract_path: Path
    territory_catalog_path: Path
    transformation_version: str
    key_rule_version: str
    source_datetime_format: str
    parquet_codec: str
    max_rejection_rate: float
    allow_empty_output: bool
    local_threads: int
    output_partitions: int


def load_silver_config(path: Path) -> SilverConfig:
    raw = _load_mapping(path, "silver")
    try:
        config = SilverConfig(
            dataset_id=_text(raw["dataset_id"], "dataset_id"),
            bronze_root=Path(_text(raw["bronze_root"], "bronze_root")),
            silver_root=Path(_text(raw["silver_root"], "silver_root")),
            contract_path=Path(_text(raw["contract_path"], "contract_path")),
            territory_catalog_path=Path(
                _text(raw["territory_catalog_path"], "territory_catalog_path")
            ),
            transformation_version=_text(raw["transformation_version"], "transformation_version"),
            key_rule_version=_text(raw["key_rule_version"], "key_rule_version"),
            source_datetime_format=_text(raw["source_datetime_format"], "source_datetime_format"),
            parquet_codec=_text(raw["parquet_codec"], "parquet_codec"),
            max_rejection_rate=_rate(raw["max_rejection_rate"]),
            allow_empty_output=_boolean(raw["allow_empty_output"], "allow_empty_output"),
            local_threads=_positive_integer(raw["local_threads"], "local_threads"),
            output_partitions=_positive_integer(raw["output_partitions"], "output_partitions"),
        )
    except KeyError as error:
        raise SilverConfigurationError(f"Missing Silver configuration key: {error}") from error
    if config.parquet_codec != "snappy":
        raise SilverConfigurationError("parquet_codec must be snappy for Silver v1")
    return config


def load_territory_catalog(path: Path) -> TerritoryCatalog:
    raw = _load_mapping(path, "catalog")
    try:
        municipalities = tuple(raw["municipalities"])
        aliases = dict(raw.get("aliases", {}))
        valle = frozenset(raw["valle_de_aburra"])
        version = _text(raw["version"], "catalog.version")
        department = _text(raw["department"], "catalog.department")
        provenance = _text(raw["provenance"], "catalog.provenance")
    except (KeyError, TypeError) as error:
        raise SilverConfigurationError(f"Invalid territorial catalog: {error}") from error
    if len(municipalities) != 125 or len(set(municipalities)) != 125:
        raise SilverConfigurationError("territorial catalog must contain 125 unique municipalities")
    if not all(isinstance(item, str) and item.strip() for item in municipalities):
        raise SilverConfigurationError("municipality names must be non-empty text")
    if not valle <= set(municipalities) or len(valle) != 10:
        raise SilverConfigurationError("Valle de Aburrá must contain 10 catalog municipalities")
    if any(target not in municipalities for target in aliases.values()):
        raise SilverConfigurationError("every territorial alias must target a canonical name")
    keys = [territory_key(item) for item in municipalities]
    if len(keys) != len(set(keys)):
        raise SilverConfigurationError("canonical municipality keys must be unique")
    digest = hashlib.sha256(
        json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return TerritoryCatalog(
        version=version,
        department=department,
        provenance=provenance,
        municipalities=municipalities,
        aliases=aliases,
        valle_de_aburra=valle,
        digest=digest,
    )


def territory_key(value: str | None) -> str | None:
    if value is None:
        return None
    decomposed = unicodedata.normalize("NFKD", value)
    without_marks = "".join(char for char in decomposed if not unicodedata.combining(char))
    normalized = re.sub(r"\s+", " ", without_marks.strip()).upper()
    return normalized or None


def normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"\s+", " ", unicodedata.normalize("NFC", value).strip())
    return normalized or None


def _load_mapping(path: Path, section: str) -> dict[str, Any]:
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        raw = document[section]
    except (OSError, KeyError, TypeError, yaml.YAMLError) as error:
        raise SilverConfigurationError(f"Could not load {path}: {error}") from error
    if not isinstance(raw, dict):
        raise SilverConfigurationError(f"{section} must be a mapping")
    return raw


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SilverConfigurationError(f"{name} must be non-empty text")
    return value


def _positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise SilverConfigurationError(f"{name} must be an integer >= 1")
    return value


def _boolean(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise SilverConfigurationError(f"{name} must be boolean")
    return value


def _rate(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise SilverConfigurationError("max_rejection_rate must be numeric")
    result = float(value)
    if not 0 <= result <= 1:
        raise SilverConfigurationError("max_rejection_rate must be between 0 and 1")
    return result
