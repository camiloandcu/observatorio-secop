"""Immutable evidence uses the same link identity in memory and PostgreSQL."""

from types import SimpleNamespace

from observatorio_secop.population import digest_json
from observatorio_secop.warehouse.config import WarehouseConfig
from observatorio_secop.warehouse.population import PopulationStore


def test_multiple_links_keep_canonical_identity_for_persist_and_retry(tmp_path):
    links = [
        {"link_id": "b", "contract_key": "key", "diagnostic": "first"},
        {"link_id": "a", "contract_key": "key", "diagnostic": "second"},
    ]
    bundle = SimpleNamespace(
        version_id="A",
        manifest_sha256="hash",
        coverage={"scope_id": "scope"},
        condition_summaries=[],
        condition_links=links,
    )
    evidence = PopulationStore._evidence(bundle)

    class Cursor:
        def __init__(self):
            self.calls = []

        def execute(self, query, params=None):
            self.calls.append(params)

        def fetchone(self):
            return None

    cursor = Cursor()
    store = PopulationStore(
        WarehouseConfig(
            silver_root=tmp_path,
            schema="silver",
            contract_table="contract_current",
            audit_table="load_audit",
            advisory_lock_id=1,
            host="unused",
            port=5432,
            dbname="unused",
            user="unused",
            password="unused",
        )
    )
    store.persist(cursor, bundle)
    inserted = cursor.calls[-2:]
    assert [values[4] for values in inserted] == ["a", "b"]
    assert [values[5].obj for values in inserted] == evidence["links"]
    assert cursor.calls[1][-1] == digest_json(evidence)
