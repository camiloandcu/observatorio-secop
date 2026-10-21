from __future__ import annotations

from unittest.mock import Mock

import pytest

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.cli import main
from observatorio_secop.warehouse.config import WarehouseConfig
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import silver_row, write_bundle
from .inventory_helpers import INVALID_CASES, cli_environment, corrupt_inventory
from .snapshot_helpers import digest, seed_registered, snapshot


@pytest.mark.parametrize("case", INVALID_CASES)
def test_invalid_cli_load_preserves_exact_relational_snapshot(
    postgres_config: WarehouseConfig, monkeypatch: pytest.MonkeyPatch, capsys, case: str
) -> None:
    root = postgres_config.silver_root
    write_bundle(root, "active", [silver_row("active", amount="123.45")])
    active = verify_current_bundle(root)
    seed_registered(postgres_config, active)
    assert WarehouseLoader(postgres_config).load(active).status == "noop"
    before = snapshot(postgres_config)
    manifest = write_bundle(root, "version-1", [silver_row("version-1", amount="999.99")])
    message = corrupt_inventory(manifest, case)
    args = cli_environment(root, monkeypatch, postgres_config)
    connect = Mock(side_effect=AssertionError("invalid inventory must not connect"))
    with monkeypatch.context() as invocation:
        invocation.setattr("psycopg.connect", connect)
        assert main([*args, "load"]) == 2
        assert message in capsys.readouterr().err
        connect.assert_not_called()
    after = snapshot(postgres_config)
    assert after == before
    assert digest(after) == digest(before)
