"""Serialized Silver selection, adoption and registered restoration."""

from __future__ import annotations

import fcntl
import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

from observatorio_secop.identity import key_set_digest
from observatorio_secop.population import PopulationScope, digest_json
from observatorio_secop.processing.silver.bronze import resolve_bronze_runs
from observatorio_secop.processing.silver.errors import PublicationError
from observatorio_secop.processing.silver.storage import canonical_json, durable_write
from observatorio_secop.warehouse.bundle import verify_manifest_bundle
from observatorio_secop.warehouse.inventory import sha256_path


@contextmanager
def selection_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".selection.lock").open("a+") as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def identity(document: dict[str, Any]) -> dict[str, str] | None:
    if not document.get("silver_version_id"):
        return None
    return {key: document[key] for key in ("silver_version_id", "manifest_sha256")}


def binding(root: Path) -> dict[str, Any] | None:
    path = root / "population_scope.json"
    if not path.exists():
        return None
    record: dict[str, Any] = json.loads(path.read_bytes())
    if (
        set(record) != {"schema_version", "scope_id", "scope"}
        or record["schema_version"] != "population-binding-v1"
        or PopulationScope.from_dict(record["scope"]).scope_id != record["scope_id"]
    ):
        raise PublicationError("Persistent population scope binding is inconsistent")
    return record


def select(
    storage: Any,
    manifest: dict[str, Any],
    *,
    expected_current: dict[str, str] | None = None,
    restore: bool = False,
) -> dict[str, Any]:
    """The caller already inventoried the immutable target; reverify under lock."""
    version_id = manifest["silver_version_id"]
    manifest_path = storage.version_path(version_id) / "manifest.json"
    target = {"silver_version_id": version_id, "manifest_sha256": sha256_path(manifest_path)}
    with selection_lock(storage.root):
        storage.verify_existing(version_id, manifest)
        verify_manifest_bundle(manifest_path, expected_hash=target["manifest_sha256"])
        if manifest.get("selection_mode") == "preview":
            raise PublicationError("Legacy preview cannot acquire authoritative selection")
        population = manifest.get("population")
        receipt = None
        if population:
            receipt = json.loads((manifest_path.parent / population["coverage_path"]).read_bytes())
            if (
                receipt.get("mode") != "authoritative_candidate"
                or receipt.get("state") != "complete"
            ):
                raise PublicationError(
                    "Preview or incomplete population cannot be selected as authoritative"
                )
            if population.get("scope_drift"):
                raise PublicationError("Observed population scope drift blocks selection")
        bound = binding(storage.root)
        if bound:
            adoption = adoption_record(storage.root, version_id, target["manifest_sha256"])
            scope_id = (
                receipt["scope_id"] if receipt else adoption.get("scope_id") if adoption else None
            )
            if scope_id != bound["scope_id"]:
                raise PublicationError(
                    "Silver target differs from persistent bound population scope"
                )
        active = identity(storage.current())
        registry_path = storage.root / "selection_registry.json"
        registry = json.loads(registry_path.read_bytes()) if registry_path.exists() else {}
        if version_id in registry and registry[version_id] != target["manifest_sha256"]:
            raise PublicationError("Registered Silver identity conflicts with its immutable hash")
        if active == target:
            if receipt and not bound:
                raise PublicationError("Active scoped selection is missing its persistent binding")
            if registry.get(version_id) != target["manifest_sha256"]:
                registry[version_id] = target["manifest_sha256"]
                durable_write(registry_path, canonical_json(registry) + b"\n")
            return cast(dict[str, Any], storage.current())
        if restore:
            if registry.get(version_id) != target["manifest_sha256"]:
                raise PublicationError(
                    "Restoration requires a previously registered exact Silver target"
                )
            if expected_current != active:
                raise PublicationError("Silver restoration expected-current identity is stale")
        elif receipt:
            if receipt.get("expected_parent") != active:
                raise PublicationError("Silver population expected-parent identity is stale")
            if active and receipt.get("baseline") is None:
                raise PublicationError(
                    "Nonempty Silver selection requires a pinned population baseline"
                )
            if active and not bound:
                raise PublicationError("Legacy population requires explicit verified adoption")
        else:
            raise PublicationError(
                "New unscoped legacy selection is denied; use a complete scoped population"
            )
        if receipt and not bound:
            durable_write(
                storage.root / "population_scope.json",
                canonical_json(
                    {
                        "schema_version": "population-binding-v1",
                        "scope_id": receipt["scope_id"],
                        "scope": receipt["scope"],
                    }
                )
                + b"\n",
            )
        # Only selected identities become restoration targets. If registration
        # fails after pointer replacement, an exact-active retry repairs it.
        storage._write_current(version_id, manifest_path)
        registry[version_id] = target["manifest_sha256"]
        durable_write(registry_path, canonical_json(registry) + b"\n")
        return cast(dict[str, Any], storage.current())


def adoption_record(root: Path, version_id: str, manifest_sha256: str) -> dict[str, Any] | None:
    path = root / "population_adoptions" / f"{version_id}.json"
    if not path.exists():
        return None
    record = json.loads(path.read_bytes())
    if record.get("manifest_sha256") != manifest_sha256:
        raise PublicationError("Adoption conflicts with original manifest identity")
    bundle = verify_manifest_bundle(
        root / "versions" / version_id / "manifest.json", expected_hash=manifest_sha256
    )
    return validate_adoption(record, bundle)


def validate_adoption(
    record: dict[str, Any], bundle: Any, *, bronze_root: Path | None = None
) -> dict[str, Any]:
    """Validate immutable adoption identity and all original members/dependencies."""
    expected = {
        "schema_version",
        "silver_version_id",
        "manifest_sha256",
        "scope",
        "scope_id",
        "keys_sha256",
        "bronze_runs",
        "coverage_known",
        "reason",
        "adoption_id",
    }
    if set(record) != expected or record["schema_version"] != "population-adoption-v1":
        raise PublicationError("Unsupported adoption schema")
    scope = PopulationScope.from_dict(record["scope"])
    document = json.loads(bundle.manifest_path.read_bytes())
    if (record["silver_version_id"], record["manifest_sha256"]) != (
        bundle.version_id,
        bundle.manifest_sha256,
    ):
        raise PublicationError("Adoption target does not match original immutable bundle")
    original = {key: value for key, value in record.items() if key != "adoption_id"}
    if (
        record["scope_id"] != scope.scope_id
        or record["adoption_id"] != digest_json(original)
        or record["coverage_known"] is not False
        or record["reason"] != "LEGACY_UNASSESSED"
        or record["keys_sha256"] != key_set_digest(bundle.table["contract_key"].to_pylist())
        or record["bronze_runs"] != document.get("bronze_runs")
        or not record["bronze_runs"]
    ):
        raise PublicationError("Adoption content conflicts with scope/keys/dependency proof")
    if any(
        not scope.contains(row["signing_date"], row["department_name"])
        for row in bundle.table.to_pylist()
    ):
        raise PublicationError("Legacy baseline contains missing/out-of-scope signing dates")
    if bronze_root is not None:
        runs = resolve_bronze_runs(
            bronze_root,
            dataset_id="jbjy-vk9h",
            contract_sha256=document.get("contract_sha256", ""),
            run_ids=tuple(item["run_id"] for item in record["bronze_runs"]),
        )
        actual = {run.run_id: run.manifest_sha256 for run in runs}
        if any(
            actual.get(item["run_id"]) != item["manifest_sha256"] for item in record["bronze_runs"]
        ):
            raise PublicationError("Adoption Bronze closure hash mismatch")
    return record


def adopt(
    storage: Any,
    version_id: str,
    manifest_sha256: str,
    scope: PopulationScope,
    *,
    bronze_root: Path,
) -> dict[str, Any]:
    """Bind an existing exact legacy snapshot without mutating its bytes."""
    path = storage.version_path(version_id) / "manifest.json"
    bundle = verify_manifest_bundle(path, expected_hash=manifest_sha256)
    document = json.loads(path.read_bytes())
    if document.get("schema_version", 1) not in (1, 2):
        raise PublicationError("Explicit adoption supports only legacy envelope 1/2")
    if document.get("selection_mode") == "preview":
        raise PublicationError("Legacy preview cannot become authoritative through adoption")
    if not document.get("bronze_runs"):
        raise PublicationError("Adoption requires complete pinned Bronze dependencies")
    if any(
        not scope.contains(row["signing_date"], row["department_name"])
        for row in bundle.table.to_pylist()
    ):
        raise PublicationError("Legacy baseline contains missing/out-of-scope signing dates")
    record = {
        "schema_version": "population-adoption-v1",
        "silver_version_id": version_id,
        "manifest_sha256": manifest_sha256,
        "scope": scope.as_dict(),
        "scope_id": scope.scope_id,
        "keys_sha256": key_set_digest(bundle.table["contract_key"].to_pylist()),
        "bronze_runs": document["bronze_runs"],
        "coverage_known": False,
        "reason": "LEGACY_UNASSESSED",
    }
    record["adoption_id"] = digest_json(record)
    validate_adoption(record, bundle, bronze_root=bronze_root)
    with selection_lock(storage.root):
        if identity(storage.current()) != {
            "silver_version_id": version_id,
            "manifest_sha256": manifest_sha256,
        }:
            raise PublicationError("Adoption target must be the exact active legacy Silver version")
        bound = binding(storage.root)
        if bound and bound["scope_id"] != scope.scope_id:
            raise PublicationError("Workspace already binds another population scope")
        previous = adoption_record(storage.root, version_id, manifest_sha256)
        if previous and previous != record:
            raise PublicationError("Immutable legacy adoption differs")
        durable_write(
            storage.root / "population_adoptions" / f"{version_id}.json",
            canonical_json(record) + b"\n",
        )
        durable_write(
            storage.root / "population_scope.json",
            canonical_json(
                {
                    "schema_version": "population-binding-v1",
                    "scope_id": scope.scope_id,
                    "scope": scope.as_dict(),
                }
            )
            + b"\n",
        )
        registry_path = storage.root / "selection_registry.json"
        registry = json.loads(registry_path.read_bytes()) if registry_path.exists() else {}
        registry[version_id] = manifest_sha256
        durable_write(registry_path, canonical_json(registry) + b"\n")
    return record
