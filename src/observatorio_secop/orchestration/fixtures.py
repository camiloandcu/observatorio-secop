"""Trusted synthetic source transport for the isolated analytical replay.

Only the generated governed reconciliation queries are understood. This transport
never delegates to a network opener and cannot read operator-supplied files.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from urllib.parse import parse_qs, urlsplit

from observatorio_secop.ingestion.config import IngestionConfig
from observatorio_secop.ingestion.contract import BronzeSourceContract
from observatorio_secop.ingestion.errors import IngestionConfigurationError, PageValidationError
from observatorio_secop.ingestion.storage import canonical_json, sha256_bytes
from observatorio_secop.ingestion.transport import SocrataTransport
from observatorio_secop.population import PopulationScope

FIXTURE_IDS = ("demo-a", "demo-b", "demo-rejected")
_SCOPE = re.compile(
    r"departamento='Antioquia' AND fecha_de_firma >= '([^']+)'"
    r" AND fecha_de_firma < '([^']+)'(?: AND id_contrato > '([A-Z0-9_-]+)')?"
)
_MEMBERS = re.compile(r"departamento='Antioquia' AND id_contrato in \((.+)\)")
_ID_LIST = re.compile(r"'[A-Z0-9_-]+'(?:,'[A-Z0-9_-]+')*")


@dataclass(frozen=True)
class FixtureExpectation:
    input_rows: int
    accepted_rows: int
    accepted_total_cop: Decimal
    rejected_rows: int
    sha256: str


def _row(identity: str, amount: str, supplier: str) -> dict[str, Any]:
    return {
        "id_contrato": identity,
        "referencia_del_contrato": f"DEMO-REF-{identity}",
        "proceso_de_compra": f"DEMO-PROC-{identity}",
        "nombre_entidad": "Entidad sintética de demostración",
        "departamento": "Antioquia",
        "ciudad": "Medellín",
        "estado_contrato": "Activo",
        "codigo_de_categoria_principal": "V1.1",
        "descripcion_del_proceso": "Proceso sintético sin datos personales",
        "tipo_de_contrato": "Servicios",
        "modalidad_de_contratacion": "Directa",
        "fecha_de_firma": "2026-01-01T00:00:00.000",
        "fecha_de_inicio_del_contrato": "2026-01-01T00:00:00.000",
        "fecha_de_fin_del_contrato": "2026-01-31T00:00:00.000",
        "valor_del_contrato": amount,
        "ultima_actualizacion": None,
        "urlproceso": {"url": f"https://community.secop.gov.co/Public/Tendering/{identity}"},
        "codigo_entidad": "1001",
        "codigo_proveedor": supplier,
        "proveedor_adjudicado": f"Proveedor sintético {supplier}",
    }


def fixture_rows(fixture_id: str) -> list[dict[str, Any]]:
    """Return fresh synthetic rows for an explicitly registered fixture identity."""
    if fixture_id not in FIXTURE_IDS:
        raise IngestionConfigurationError("Unsupported orchestration fixture identifier")
    rows = [_row("A", "100.00", "2001"), _row("B", "200.00", "2002")]
    if fixture_id == "demo-b":
        rows[0]["valor_del_contrato"] = "150.00"
        rows.append(_row("C", "50.00", "2001"))
    elif fixture_id == "demo-rejected":
        # A valid source number intentionally violates Silver's nonnegative rule.
        rows[0]["valor_del_contrato"] = "-1.00"
    return rows


def fixture_expected(fixture_id: str) -> FixtureExpectation:
    rows = fixture_rows(fixture_id)
    accepted = [row for row in rows if Decimal(row["valor_del_contrato"]) >= 0]
    return FixtureExpectation(
        input_rows=len(rows),
        accepted_rows=len(accepted),
        accepted_total_cop=sum((Decimal(r["valor_del_contrato"]) for r in accepted), Decimal(0)),
        rejected_rows=len(rows) - len(accepted),
        sha256=sha256_bytes(canonical_json({"fixture_id": fixture_id, "rows": rows})),
    )


class _Response:
    headers: dict[str, str] = {"Content-Type": "application/json"}
    status = 200

    def __init__(self, payload: Any) -> None:
        self.raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    def read(self, limit: int) -> bytes:
        return self.raw[:limit]


def fixture_transport(
    config: IngestionConfig, contract: BronzeSourceContract, fixture_id: str
) -> SocrataTransport:
    """Use real bounded transport/validation/accounting against a local source stub."""
    rows = fixture_rows(fixture_id)
    metadata = {
        "id": contract.dataset_id,
        "columns": [
            {"fieldName": name, "dataTypeName": details["declared_type"]}
            for name, details in contract.columns.items()
        ],
    }
    endpoint = urlsplit(contract.resource_url)

    def opener(request: Any, timeout: float) -> _Response:
        del timeout
        if request.full_url == contract.metadata_url:
            return _Response(metadata)
        url = urlsplit(request.full_url)
        if (url.scheme, url.netloc, url.path) != (endpoint.scheme, endpoint.netloc, endpoint.path):
            raise PageValidationError("Offline fixture denied an external data request")
        params = parse_qs(url.query, strict_parsing=True)
        if (
            set(params) != {"$select", "$where", "$order", "$limit"}
            or any(len(value) != 1 for value in params.values())
            or params["$select"] != [",".join(config.projection)]
            or params["$order"] != ["id_contrato ASC"]
        ):
            raise PageValidationError("Offline fixture requires a governed population query")
        try:
            limit = int(params["$limit"][0])
        except ValueError as error:
            raise PageValidationError("Offline fixture requires a bounded page") from error
        if not 0 < limit <= config.max_page_size:
            raise PageValidationError("Offline fixture requires a bounded page")
        predicate = params["$where"][0]
        if match := _SCOPE.fullmatch(predicate):
            scope = PopulationScope(match[1], match[2])
            selected = [
                row
                for row in rows
                if scope.contains(row["fecha_de_firma"], row["departamento"])
                and (match[3] is None or row["id_contrato"] > match[3])
            ]
        elif (match := _MEMBERS.fullmatch(predicate)) and _ID_LIST.fullmatch(match[1]):
            identities = {value.strip("'") for value in match[1].split(",")}
            selected = [row for row in rows if row["id_contrato"] in identities]
        else:
            raise PageValidationError("Offline fixture requires a governed population query")
        return _Response(
            [{name: row.get(name) for name in config.projection} for row in selected[:limit]]
        )

    return SocrataTransport(config, contract, opener=opener, sleeper=lambda _: None, token=None)
