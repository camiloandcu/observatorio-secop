"""Validated local configuration for the analytical warehouse."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from observatorio_secop.warehouse.errors import WarehouseConfigurationError

IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


@dataclass(frozen=True)
class WarehouseConfig:
    silver_root: Path
    schema: str
    contract_table: str
    audit_table: str
    advisory_lock_id: int
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @property
    def connection_kwargs(self) -> dict[str, str | int]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "connect_timeout": 10,
        }


def load_warehouse_config(
    path: Path, *, environment: Mapping[str, str] | None = None
) -> WarehouseConfig:
    env = os.environ if environment is None else environment
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        raw = document["warehouse"]
    except (OSError, KeyError, TypeError, yaml.YAMLError) as error:
        raise WarehouseConfigurationError(
            f"Could not load warehouse configuration: {error}"
        ) from error
    if not isinstance(raw, dict):
        raise WarehouseConfigurationError("warehouse must be a mapping")

    silver_root = Path(env.get("SILVER_ROOT", _text(raw.get("silver_root"), "silver_root")))
    schema = _identifier(raw.get("schema"), "schema")
    contract_table = _identifier(raw.get("contract_table"), "contract_table")
    audit_table = _identifier(raw.get("audit_table"), "audit_table")
    lock_id = _integer(raw.get("advisory_lock_id"), "advisory_lock_id")
    return WarehouseConfig(
        silver_root=silver_root,
        schema=schema,
        contract_table=contract_table,
        audit_table=audit_table,
        advisory_lock_id=lock_id,
        host=env.get("POSTGRES_HOST", "127.0.0.1"),
        port=_integer(env.get("POSTGRES_PORT", "5432"), "POSTGRES_PORT"),
        dbname=_required_env(env, "POSTGRES_DB"),
        user=_required_env(env, "POSTGRES_USER"),
        password=_required_env(env, "POSTGRES_PASSWORD"),
    )


def _required_env(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name, "")
    if not value.strip():
        raise WarehouseConfigurationError(f"Missing required environment variable: {name}")
    return value


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WarehouseConfigurationError(f"{name} must be non-empty text")
    return value


def _identifier(value: Any, name: str) -> str:
    result = _text(value, name)
    if not IDENTIFIER.fullmatch(result):
        raise WarehouseConfigurationError(f"{name} must be a safe PostgreSQL identifier")
    return result


def _integer(value: Any, name: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as error:
        raise WarehouseConfigurationError(f"{name} must be an integer") from error
    if result < 1:
        raise WarehouseConfigurationError(f"{name} must be greater than zero")
    return result
