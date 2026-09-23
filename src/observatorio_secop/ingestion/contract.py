"""Adapt the frozen source contract to ingestion decisions."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from observatorio_secop.ingestion.errors import SourceContractError
from observatorio_secop.source_profile.contract import SENSITIVE_FIELD_PATTERN


@dataclass(frozen=True)
class BronzeSourceContract:
    dataset_id: str
    api_base_url: str
    department_field: str
    department_value: str
    identity_field: str
    watermark_field: str
    tie_breaker: tuple[str, ...]
    columns: dict[str, dict[str, Any]]
    critical_fields: tuple[str, ...]
    contract_hash: str
    raw: dict[str, Any]

    @property
    def metadata_url(self) -> str:
        return f"{self.api_base_url}/api/views/{self.dataset_id}"

    @property
    def resource_url(self) -> str:
        return f"{self.api_base_url}/resource/{self.dataset_id}.json"


def load_bronze_contract(path: Path, projection: tuple[str, ...]) -> BronzeSourceContract:
    try:
        raw_bytes = path.read_bytes()
        document = yaml.safe_load(raw_bytes)
    except (OSError, yaml.YAMLError) as error:
        raise SourceContractError(f"Could not load source contract {path}: {error}") from error
    if not isinstance(document, dict):
        raise SourceContractError("Source contract must be a mapping")
    try:
        columns = {item["field"]: item for item in document["columns"]}
        identity_fields = tuple(document["identity"]["fields"])
        tie_breaker = tuple(document["incremental_cursor"]["tie_breaker"])
        contract = BronzeSourceContract(
            dataset_id=document["source"]["dataset_id"],
            api_base_url=document["source"]["api_base_url"].rstrip("/"),
            department_field=document["territory"]["department_field"],
            department_value=document["territory"]["department_value"],
            identity_field=identity_fields[0],
            watermark_field=document["incremental_cursor"]["field"],
            tie_breaker=tie_breaker,
            columns=columns,
            critical_fields=tuple(
                sorted(item["field"] for item in document["columns"] if item.get("critical"))
            ),
            contract_hash=hashlib.sha256(raw_bytes).hexdigest(),
            raw=document,
        )
    except (KeyError, IndexError, TypeError) as error:
        raise SourceContractError(f"Source contract lacks a required decision: {error}") from error
    if contract.dataset_id != "jbjy-vk9h":
        raise SourceContractError("Source contract identifies an unexpected dataset")
    if document["identity"].get("status") != "resolved" or len(identity_fields) != 1:
        raise SourceContractError("Bronze ingestion requires one resolved natural identity")
    if document["incremental_cursor"].get("status") != "resolved":
        raise SourceContractError("Bronze ingestion requires a resolved incremental cursor")
    _validate_projection(contract, projection)
    return contract


def _validate_projection(contract: BronzeSourceContract, projection: tuple[str, ...]) -> None:
    unknown = sorted(set(projection) - set(contract.columns))
    if unknown:
        raise SourceContractError(f"Bronze projection references unknown columns: {unknown}")
    required = {
        contract.department_field,
        contract.identity_field,
        contract.watermark_field,
        *contract.tie_breaker,
    }
    missing = sorted(required - set(projection))
    if missing:
        raise SourceContractError(f"Bronze projection is missing required columns: {missing}")
    sensitive = sorted(field for field in projection if SENSITIVE_FIELD_PATTERN.search(field))
    if sensitive:
        raise SourceContractError(
            f"Bronze projection contains sensitive field categories: {sensitive}"
        )
