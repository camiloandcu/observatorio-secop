"""Build a local native Power BI project using one trusted publication descriptor."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bi.analytics import enrich_model
from bi.metadata import DERIVED_FIELDS
from bi.metadata import EXPRESSION as METADATA_EXPRESSION
from bi.report import generate

ROOT = Path(__file__).resolve().parents[1]
AUTHORING_CLI = "@microsoft/powerbi-report-authoring-cli@0.5.0"


def m_text(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def publication_literal(descriptor: dict) -> str:
    """Validate the frozen operation identity; availability is checked in M."""
    identity = descriptor.get("publication_id", "")
    if (
        type(descriptor.get("version")) is not int
        or descriptor["version"] != 1
        or not isinstance(identity, str)
        or not re.fullmatch(r"[0-9a-f]{32}", identity)
    ):
        raise ValueError("A version-1 trusted publication descriptor is required")
    if descriptor.get("read_namespace") != "publication_" + identity:
        raise ValueError("Descriptor namespace does not match its publication")
    times = []
    for field in ("resolved_at", "operation_expires_at"):
        value = datetime.fromisoformat(descriptor[field].replace("Z", "+00:00"))
        if value.tzinfo is None or value.utcoffset() != timedelta(0):
            raise ValueError("Descriptor timestamps must explicitly identify UTC")
        times.append(value)
    if times[1] - times[0] != timedelta(hours=1):
        raise ValueError("Descriptor lifetime must be exactly one hour")
    if not times[0] <= datetime.now(UTC) < times[1]:
        raise ValueError("Resolve a fresh descriptor before building the complete Import")
    return (
        "["
        + ", ".join(
            f"{key} = {m_text(descriptor[key])}"
            for key in ("publication_id", "resolved_at", "operation_expires_at")
        )
        + "]"
    )


def projection_kind(column: dict) -> str:
    annotations = {a["name"]: a["value"] for a in column.get("annotations", [])}
    sql_type = annotations.get("Secop.SourceSqlType", "")
    if sql_type.startswith("numeric") and column["name"].endswith("_cop"):
        return "currency"
    if sql_type == "timestamp with time zone":
        return "utc"
    if sql_type in {"json", "jsonb", "ARRAY"}:
        return "json"
    return "plain"


def compile_model(descriptor: dict, server: str, database: str) -> dict:
    """Compile independent queries from one descriptor and one reader source."""
    literal = publication_literal(descriptor)
    if not server.strip() or not database.strip():
        raise ValueError("A reader server and database are required")
    model = json.loads((ROOT / "bi/model/base-model.bim").read_text())
    reader = (ROOT / "docs/examples/publication-import.pq").read_text()
    constants = {
        "PublicationDescriptor": literal,
        "PublicationServer": m_text(server),
        "PublicationDatabase": m_text(database),
    }
    for table in model["model"]["tables"]:
        partition = table["partitions"][0]
        if table["name"] == "ImportContext":
            body = partition["source"]["expression"]
            if isinstance(body, list):
                body = "\n".join(body)
            partition["source"]["expression"] = body
            continue
        fields, transformations = [], []
        for column in table["columns"]:
            derived = next(
                (a["value"] for a in column.get("annotations", []) if a["name"] == "Secop.Derived"),
                None,
            )
            if derived is not None:
                if (
                    table["name"] != "publication_metadata"
                    or DERIVED_FIELDS.get(column["name"]) != derived
                ):
                    raise ValueError("Unsupported derived publication metadata field")
                continue
            name, kind = column["sourceColumn"], projection_kind(column)
            fields.append("[name = " + m_text(name) + ", kind = " + m_text(kind) + "]")
            conversion = None
            if kind == "currency":
                conversion = 'Currency.From(_, "en-US")'
                value_type = "Currency.Type"
            elif kind == "utc":
                conversion = (
                    "DateTimeZone.RemoveZone(DateTimeZone.SwitchZone(DateTimeZone.FromText(_), 0))"
                )
                value_type = "type nullable datetime"
            elif column["dataType"] == "dateTime":
                conversion = "DateTime.From(_)"
                value_type = "type nullable datetime"
            if conversion:
                transformations.append(
                    "{"
                    + m_text(name)
                    + ", each if _ = null then null else "
                    + conversion
                    + ", "
                    + value_type
                    + "}"
                )
        expression = (
            "let ReadPublication = (\n"
            + reader
            + "\n), "
            + "PublicationProjection = {"
            + ", ".join(fields)
            + "}, "
            + "RawSource = ReadPublication(PublicationDescriptor, "
            + m_text(table["name"])
            + ", PublicationServer, PublicationDatabase, PublicationProjection)"
        )
        expression += (
            METADATA_EXPRESSION
            if table["name"] == "publication_metadata"
            else ", Source = RawSource"
        )
        if transformations:
            expression += ", Typed = Table.TransformColumns(Source, {"
            expression += ", ".join(transformations) + "})"
        else:
            expression += ", Typed = Source"
        expression += ", Buffered = Table.Buffer(Typed)"
        expression += (
            ", Checked = if Table.IsEmpty(Table.SelectRowsWithErrors(Buffered)) then Buffered "
            'else error Error.Record("PUBLICATION_TYPE_ERROR", '
            '"Una columna no coincide con el contrato del reporte; '
            'reinicia el Import completo.", null) '
            "in Checked"
        )
        partition["source"]["expression"] = expression
    model["model"]["expressions"] = [
        {**e, "expression": constants[e["name"]]}
        for e in model["model"]["expressions"]
        if e["name"] in constants
    ]
    return enrich_model(model)


def build(
    output: Path,
    descriptor: dict,
    server: str,
    database: str,
    *,
    synthetic: bool = False,
    name: str = "observatorio-secop",
) -> Path:
    """Write model definitions only; native Desktop owns credentials and Import."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", name):
        raise ValueError("Project name must be a safe filename stem")
    model = compile_model(descriptor, server, database)
    model["name"] = name
    output = output.resolve()
    if output.exists() and any(output.iterdir()):
        raise ValueError("Use a new output directory to preserve existing Desktop state")
    output.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "npm",
            "exec",
            "--yes",
            "--package",
            AUTHORING_CLI,
            "--",
            "powerbi-report-author",
            "scaffold",
            str(output),
            "--name",
            name,
            "--page-name",
            "Panorama",
            "--model-path",
            f"../{name}.SemanticModel",
            "--offline",
        ],
        check=True,
    )
    folder = output / f"{name}.SemanticModel"
    folder.mkdir()
    (folder / "definition.pbism").write_text(
        json.dumps(
            {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/"
                "definitionProperties/1.0.0/schema.json",
                "version": "4.0",
            },
            indent=2,
        )
        + "\n"
    )
    (folder / "model.bim").write_text(json.dumps(model, ensure_ascii=False, indent=2) + "\n")
    generate(output / f"{name}.Report", synthetic=synthetic)
    return output / f"{name}.pbip"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--name", default="observatorio-secop", help="Safe project filename stem")
    parser.add_argument("--synthetic", action="store_true", help="Disclose synthetic test data")
    args = parser.parse_args()
    result = build(
        args.output,
        json.loads(args.descriptor.read_text()),
        args.server,
        args.database,
        synthetic=args.synthetic,
        name=args.name,
    )
    print(f"Native project created: {result}. No data imported and no credentials embedded.")
