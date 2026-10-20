from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from observatorio_secop.ingestion.config import load_ingestion_config
from observatorio_secop.ingestion.contract import load_bronze_contract
from observatorio_secop.ingestion.errors import (
    IngestionConfigurationError,
    PageValidationError,
    RequestBudgetExceeded,
    ResumeError,
)
from observatorio_secop.ingestion.refresh import IdentityRefresh
from observatorio_secop.ingestion.transport import TransportResponse

from .conftest import rewrite_manifest, silver_row, write_bundle

ROOT = Path(__file__).resolve().parents[2]


def source_row(identity, **updates):
    return {
        "id_contrato": identity,
        "departamento": "Antioquia",
        "ciudad": "Medellín",
        "ultima_actualizacion": None,
        "fecha_de_firma": None,
        "valor_del_contrato": "10.00",
        "codigo_entidad": "001",
        "codigo_proveedor": "002",
        "proveedor_adjudicado": None,
        **updates,
    }


class RefreshTransport:
    token = None
    max_requests = None
    requests = 0

    def __init__(self, contract, pages):
        self.contract = contract
        self.pages = iter(pages)
        self.queries = []

    def _response(self, value):
        if self.max_requests is not None and self.requests >= self.max_requests:
            raise RequestBudgetExceeded("HTTP budget exhausted")
        self.requests += 1
        return TransportResponse(json.dumps(value).encode(), value, 200, 1, 0, {})

    def fetch_metadata(self):
        return self._response(
            {
                "id": self.contract.dataset_id,
                "columns": [
                    {"fieldName": name, "dataTypeName": column["declared_type"]}
                    for name, column in self.contract.columns.items()
                ],
            }
        )

    def query(self, params):
        # Reserve before consuming the next simulated page.
        if self.max_requests is not None and self.requests >= self.max_requests:
            raise RequestBudgetExceeded("HTTP budget exhausted")
        self.queries.append(params)
        return self._response(next(self.pages))


def refresh_fixture(tmp_path, ids, pages):
    config = replace(
        load_ingestion_config(ROOT / "config/bronze_ingestion.yaml"),
        bronze_root=tmp_path / "bronze",
        contract_path=ROOT / "contracts/secop_source.yaml",
        page_size=2,
        max_rows=10,
    )
    contract = load_bronze_contract(config.contract_path, config.projection)
    baseline = write_bundle(
        tmp_path / "silver",
        "baseline",
        [silver_row("baseline", f"key-{identity}", contract_id=identity) for identity in ids],
    )
    rewrite_manifest(
        baseline,
        lambda value: value.update(
            bronze_runs=[{"run_id": "legacy-baseline", "manifest_sha256": "b" * 64}]
        ),
    )
    transport = RefreshTransport(contract, pages)
    refresh = IdentityRefresh(
        config, contract, transport=transport, uuid_factory=lambda: "refresh-1"
    )
    with refresh.state.connect() as connection:
        connection.execute(
            "INSERT INTO checkpoints VALUES (?, 'primary', ?, 'old-run')",
            (contract.dataset_id, "2026-01-01T00:00:00Z"),
        )
    return refresh, baseline, transport


def checkpoint_rows(refresh):
    with refresh.state.connect() as connection:
        return connection.execute("SELECT * FROM checkpoints ORDER BY 1,2").fetchall()


def test_refresh_missing_ids_and_null_dates_keep_checkpoint_and_complete_evidence(tmp_path) -> None:
    refresh, baseline, transport = refresh_fixture(
        tmp_path, ["A'1", "B", "C"], [[source_row("A'1")]]
    )
    before = checkpoint_rows(refresh)
    result = refresh.refresh(baseline, ["B", "A'1"])
    assert result.status == "committed" and result.range_complete
    assert result.refresh["missing_ids"] == ["B"]
    assert result.refresh["returned_ids"] == ["A'1"]
    assert result.refresh["http_attempts"] == 2
    assert result.checkpoint_candidate is None and checkpoint_rows(refresh) == before
    assert (
        transport.queries[0]["$where"] == "departamento='Antioquia' AND id_contrato in ('A''1','B')"
    )
    assert "ultima_actualizacion" not in transport.queries[0]["$where"]
    assert refresh.refresh(baseline, ["A'1", "B"], resume=result.run_id).run_id == result.run_id
    assert len(transport.queries) == 1 and checkpoint_rows(refresh) == before
    with pytest.raises(ResumeError, match="changed"):
        refresh.refresh(baseline, ["C"], resume=result.run_id)


def test_refresh_budget_truncates_and_resumes_without_skipping_ids(tmp_path) -> None:
    refresh, baseline, transport = refresh_fixture(
        tmp_path,
        ["A", "B", "C"],
        [
            [source_row("A"), source_row("B")],
            [source_row("C")],
        ],
    )
    before = checkpoint_rows(refresh)
    first = refresh.refresh(baseline, ["A", "B", "C"], max_requests=1)
    assert first.truncated and first.status == "truncated_recoverable" and not first.pages
    second = refresh.refresh(baseline, ["A", "B", "C"], max_requests=1, resume=first.run_id)
    assert second.truncated and len(second.pages) == 1
    third = refresh.refresh(baseline, ["A", "B", "C"], max_requests=1, resume=first.run_id)
    assert third.status == "committed" and third.refresh["returned_ids"] == ["A", "B", "C"]
    assert third.refresh["http_attempts"] == 3
    assert len(transport.queries) == 2 and checkpoint_rows(refresh) == before


@pytest.mark.parametrize("bad_ids", [[], ["A", "A"], ["outside"], ["A"] * 11])
def test_refresh_rejects_invalid_membership_before_requests(tmp_path, bad_ids) -> None:
    refresh, baseline, transport = refresh_fixture(tmp_path, ["A"], [])
    before = checkpoint_rows(refresh)
    with pytest.raises(IngestionConfigurationError):
        refresh.refresh(baseline, bad_ids)
    assert not transport.queries and checkpoint_rows(refresh) == before


@pytest.mark.parametrize(
    "page",
    [
        [source_row("outside")],
        [source_row("A", departamento="Other")],
        [source_row("A"), source_row("A")],
    ],
)
def test_refresh_rejects_invalid_response_without_checkpoint_or_commit(tmp_path, page) -> None:
    refresh, baseline, _ = refresh_fixture(tmp_path, ["A", "B"], [page])
    before = checkpoint_rows(refresh)
    with pytest.raises(PageValidationError):
        refresh.refresh(baseline, ["A", "B"])
    assert checkpoint_rows(refresh) == before
    assert not list(refresh.storage.runs_root.glob("*/manifest.json"))


def test_refresh_resume_rejects_progress_tampering(tmp_path) -> None:
    refresh, baseline, _ = refresh_fixture(tmp_path, ["A", "B", "C"], [[source_row("A")]])
    first = refresh.refresh(baseline, ["A", "B", "C"], max_requests=1)
    path = refresh.storage.staging_path(first.run_id) / "page-state.json"
    value = json.loads(path.read_text())
    value["refresh"]["next_id_offset"] = 2
    path.write_text(json.dumps(value))
    with pytest.raises(ResumeError, match="progress"):
        refresh.refresh(baseline, ["A", "B", "C"], max_requests=1, resume=first.run_id)
