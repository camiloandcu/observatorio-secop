from pathlib import Path

import pytest
import yaml

from observatorio_secop.processing.silver.cli import main as silver_main
from observatorio_secop.processing.silver.config import load_silver_config
from observatorio_secop.processing.silver.errors import SilverConfigurationError
from observatorio_secop.warehouse.cli import main as warehouse_main
from observatorio_secop.warehouse.config import load_warehouse_config
from observatorio_secop.warehouse.errors import WarehouseConfigurationError

ROOT = Path(__file__).resolve().parents[1]
DATABASE_ENV = {"POSTGRES_DB": "test", "POSTGRES_USER": "test", "POSTGRES_PASSWORD": "test-only"}


def configs(tmp_path: Path, silver_root: str) -> tuple[Path, Path]:
    paths = []
    for name, section in [("silver_quality", "silver"), ("warehouse", "warehouse")]:
        document = yaml.safe_load((ROOT / f"config/{name}.yaml").read_text())
        document[section]["silver_root"] = silver_root
        path = tmp_path / f"{name}.yaml"
        path.write_text(yaml.safe_dump(document))
        paths.append(path)
    return paths[0], paths[1]


def test_documented_defaults_select_the_producer_root() -> None:
    silver = load_silver_config(ROOT / "config/silver_quality.yaml", environment={})
    warehouse = load_warehouse_config(ROOT / "config/warehouse.yaml", environment=DATABASE_ENV)
    example = dict(
        line.split("=", 1)
        for line in (ROOT / ".env.example").read_text().splitlines()
        if line and not line.startswith("#")
    )
    assert silver.silver_root == warehouse.silver_root == Path(example["SILVER_ROOT"])
    assert silver.silver_root == Path("data/silver/secop_contracts")


@pytest.mark.parametrize("override", [None, "custom/exported"])
def test_selected_yaml_and_environment_have_identical_precedence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, override: str | None
) -> None:
    silver_path, warehouse_path = configs(tmp_path, "custom/yaml")
    monkeypatch.chdir(tmp_path)
    # A local dotenv file alone must not override either command's configuration.
    (tmp_path / ".env").write_text("SILVER_ROOT=unexpected/dotenv\n")
    environment = DATABASE_ENV | ({"SILVER_ROOT": override} if override else {})
    silver = load_silver_config(silver_path, environment=environment)
    warehouse = load_warehouse_config(warehouse_path, environment=environment)
    assert silver.silver_root.resolve() == warehouse.silver_root.resolve()
    assert silver.silver_root.resolve() == tmp_path / (override or "custom/yaml")


@pytest.mark.parametrize("value", ["", "   "])
def test_empty_root_override_is_rejected_by_both_loaders(value: str) -> None:
    environment = DATABASE_ENV | {"SILVER_ROOT": value}
    with pytest.raises(SilverConfigurationError, match="SILVER_ROOT"):
        load_silver_config(ROOT / "config/silver_quality.yaml", environment=environment)
    with pytest.raises(WarehouseConfigurationError, match="SILVER_ROOT"):
        load_warehouse_config(ROOT / "config/warehouse.yaml", environment=environment)


def test_cli_config_selection_does_not_override_exported_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    silver_path, warehouse_path = configs(tmp_path, str(tmp_path / "yaml"))
    for name, value in DATABASE_ENV.items():
        monkeypatch.setenv(name, value)
    exported = tmp_path / "exported"
    monkeypatch.setenv("SILVER_ROOT", str(exported))
    assert silver_main(["--config", str(silver_path), "inspect"]) == 0
    assert str(exported) in capsys.readouterr().out
    assert warehouse_main(["--config", str(warehouse_path), "verify"]) == 2
    assert str(exported / "current.json") in capsys.readouterr().err
    assert not exported.exists()
