from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.config import load_warehouse_config
from observatorio_secop.warehouse.errors import WarehouseConfigurationError
from observatorio_secop.warehouse.loader import WarehouseLoader


@pytest.mark.parametrize(
    "changes",
    [
        {"audit_table": "x" * 53},
        {"contract_table": "x" * 64},
        {"schema": "x" * 64},
        {"contract_table": "load_audit"},
        {"contract_table": "load_audit_activation"},
        {"contract_table": "load_audit_active"},
        {"contract_table": "load_audit_identity"},
        {"audit_table": "unsafe;table"},
    ],
)
def test_invalid_or_colliding_names_fail_before_connecting(silver_root: Path, changes) -> None:
    config = load_warehouse_config(
        Path("config/warehouse.yaml"),
        environment={"POSTGRES_DB": "test", "POSTGRES_USER": "test", "POSTGRES_PASSWORD": "test"},
    )
    bundle = verify_current_bundle(silver_root)
    with patch("observatorio_secop.warehouse.loader.psycopg.connect") as connect:
        with pytest.raises(WarehouseConfigurationError):
            WarehouseLoader(replace(config, **changes)).load(bundle)
        connect.assert_not_called()
