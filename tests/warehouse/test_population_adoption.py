"""Physical legacy closure -> explicit adoption -> scoped SQL successor -> restore."""

import hashlib
import json

import psycopg
import pytest

from observatorio_secop.ingestion.compatibility import CompatibilityRegistry
from observatorio_secop.population import PopulationScope
from observatorio_secop.processing.silver.selection import adopt
from observatorio_secop.processing.silver.storage import SilverStorage
from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.errors import WarehouseConflictError, WarehouseLoadError
from observatorio_secop.warehouse.inventory import sha256_path
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import (
    population_row,
    rewrite_manifest,
    write_identity_bundle,
    write_population_bundle,
)
from .snapshot_helpers import seed_registered
from .test_population_postgres import snapshot


def test_verified_legacy_adoption_scoped_transition_and_unknown_restore(postgres_config, tmp_path):
    config = postgres_config
    bronze = tmp_path / "bronze"
    page = bronze / "runs/run-1/pages/primary-00001.json"
    page.parent.mkdir(parents=True)
    page.write_text(json.dumps([{"id_contrato": "CO1.PCCNTR.1"}]))
    profile = CompatibilityRegistry().profiles[-1]
    bronze_manifest = page.parent.parent / "manifest.json"
    bronze_manifest.write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "dataset_id": "jbjy-vk9h",
                "status": "committed",
                "contract_sha256": profile.contract_sha256,
                "parameters": {"projection": list(profile.projection)},
                "counts": {"written": 1},
                "pages": [
                    {
                        "path": "pages/primary-00001.json",
                        "lane": "primary",
                        "sequence": 1,
                        "fetched_at_utc": "2026-01-03T00:00:00Z",
                        "sha256": sha256_path(page),
                        "row_count": 1,
                    }
                ],
            }
        )
    )
    manifest = write_identity_bundle(config.silver_root, "legacy-A", [population_row("legacy-A")])
    rewrite_manifest(
        manifest,
        lambda document: document.update(
            contract_sha256=profile.contract_sha256,
            bronze_runs=[{"run_id": "run-1", "manifest_sha256": sha256_path(bronze_manifest)}],
        ),
    )
    a = verify_current_bundle(config.silver_root)
    seed_registered(config, a)
    loader = WarehouseLoader(config)
    assert loader.load(a).status == "noop"
    before = snapshot(config)
    original_hash = hashlib.sha256(manifest.read_bytes()).hexdigest()
    scope = PopulationScope("2026-01-01", "2027-01-01")
    record = adopt(
        SilverStorage(config.silver_root),
        a.version_id,
        a.manifest_sha256,
        scope,
        bronze_root=bronze,
    )
    with psycopg.connect(**config.connection_kwargs) as connection:
        connection.execute(
            f'CREATE FUNCTION "{config.schema}".reject_adoption() RETURNS trigger '
            "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'induced adoption failure'; END $$"
        )
        connection.execute(
            f'CREATE TRIGGER reject_adoption BEFORE INSERT ON "{config.schema}".'
            "contract_current_population_adoption "
            f'FOR EACH ROW EXECUTE FUNCTION "{config.schema}".reject_adoption()'
        )
    with pytest.raises(WarehouseLoadError, match="PostgreSQL adoption failed"):
        loader.adopt(a, record, bronze_root=bronze)
    assert snapshot(config) == before
    with psycopg.connect(**config.connection_kwargs) as connection:
        connection.execute(
            f'DROP TRIGGER reject_adoption ON "{config.schema}".'
            "contract_current_population_adoption"
        )
    loader.adopt(a, record, bronze_root=bronze)
    assert hashlib.sha256(manifest.read_bytes()).hexdigest() == original_hash
    assert snapshot(config)["contract_current"] == before["contract_current"]
    assert loader.adopt(a, record, bronze_root=bronze)["scope_id"] == scope.scope_id
    write_population_bundle(config.silver_root, "B", [population_row("B", amount="200.00")])
    b = verify_current_bundle(config.silver_root)
    assert loader.load(b).status == "applied"
    assert (
        loader.load(a, restore=True, expected_current=(b.version_id, b.manifest_sha256)).status
        == "applied"
    )
    restored = snapshot(config)
    assert restored["contract_current"] == before["contract_current"]
    assert loader.load(a).status == "noop"
    with psycopg.connect(**config.connection_kwargs) as connection:
        (receipt,) = connection.execute(
            f'SELECT receipt FROM "{config.schema}".contract_current_population '
            "WHERE silver_version_id='legacy-A'"
        ).fetchone()
    assert receipt == {"coverage_known": False, "state": None, "reason": "LEGACY_UNASSESSED"}
    with pytest.raises(WarehouseConflictError, match="expected current"):
        loader.load(b, restore=True, expected_current=(b.version_id, b.manifest_sha256))
    assert snapshot(config) == restored
