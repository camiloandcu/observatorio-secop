from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DBT_ROOT = ROOT / "dbt"


def test_every_declared_model_and_column_has_a_description() -> None:
    documents = [
        yaml.safe_load((DBT_ROOT / "models/sources.yml").read_text(encoding="utf-8")),
        yaml.safe_load((DBT_ROOT / "models/models.yml").read_text(encoding="utf-8")),
    ]
    resources = documents[0]["sources"][0]["tables"] + documents[1]["models"]
    for resource in resources:
        assert resource.get("description"), resource["name"]
        for column in resource.get("columns", []):
            assert column.get("description"), f"{resource['name']}.{column['name']}"


def test_all_expected_star_models_exist() -> None:
    expected = {
        "dim_entity.sql",
        "dim_supplier.sql",
        "dim_municipality.sql",
        "dim_modality.sql",
        "dim_date.sql",
        "fact_contract.sql",
    }
    assert expected <= {path.name for path in (DBT_ROOT / "models").rglob("*.sql")}


def test_business_rules_are_centralized() -> None:
    intermediate = (DBT_ROOT / "models/intermediate/int_contract_enriched.sql").read_text(
        encoding="utf-8"
    )
    assert "duration_days" in intermediate
    assert "entity_natural_key" in intermediate
    for path in (DBT_ROOT / "models/dimensions").glob("*.sql"):
        assert "duration_days" not in path.read_text(encoding="utf-8")


def test_profile_example_uses_environment_variables() -> None:
    profile = (DBT_ROOT / "profiles.yml.example").read_text(encoding="utf-8")
    assert "env_var('POSTGRES_PASSWORD')" in profile
    assert "local_only_change_me" not in profile
    assert not (DBT_ROOT / "profiles.yml").exists()


def test_full_parser_is_used_to_preserve_full_refresh_dependencies() -> None:
    project = yaml.safe_load((DBT_ROOT / "dbt_project.yml").read_text(encoding="utf-8"))
    assert project["flags"]["static_parser"] is False
